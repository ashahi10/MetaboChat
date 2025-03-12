#!/usr/bin/env python3
import xml.etree.ElementTree as ET
import argparse
import json
import random
import os
from collections import defaultdict

# List of XML file paths
DATA_FILES = [
    "./data/hmdb_metabolites.xml",
    "./data/feces_metabolites.xml",
    "./data/hmdb_proteins.xml",
    "./data/saliva_metabolites.xml",
    "./data/serum_metabolites.xml",
    "./data/sweat_metabolites.xml",
    "./data/urine_metabolites.xml"
]

def process_xml_file(xml_file, max_samples=10):
    """
    Process an XML file using iterparse to count each tag and sample max_samples random entries per tag.
    Uses reservoir sampling for random sampling.
    Returns two dictionaries:
      - tag_counts: mapping tag -> occurrence count.
      - tag_samples: mapping tag -> list of sample dicts.
    """
    tag_counts = defaultdict(int)
    tag_samples = defaultdict(list)
    
    # Process the file element by element (streaming mode)
    for event, elem in ET.iterparse(xml_file, events=('end',)):
        tag = elem.tag
        tag_counts[tag] += 1
        
        # Create a sample dict with attributes, text, and children tags.
        sample = {
            'attributes': dict(elem.attrib),
            'text': (elem.text or "").strip(),
            'children': [child.tag for child in list(elem)]
        }
        
        # Reservoir sampling: if we haven't reached max_samples, append; otherwise, randomly replace.
        if len(tag_samples[tag]) < max_samples:
            tag_samples[tag].append(sample)
        else:
            rand_index = random.randint(0, tag_counts[tag] - 1)
            if rand_index < max_samples:
                tag_samples[tag][rand_index] = sample
        
        # Free memory for large XML files.
        elem.clear()
    
    # Convert defaultdicts to regular dicts for JSON serialization.
    return dict(tag_counts), dict(tag_samples)

def main(output_dir, max_samples):
    # Ensure the output directory exists.
    os.makedirs(output_dir, exist_ok=True)
    
    for xml_file in DATA_FILES:
        print(f"Processing file: {xml_file}")
        counts, samples = process_xml_file(xml_file, max_samples=max_samples)
        
        summary = {
            "tag_counts": counts,
            "tag_samples": samples
        }
        
        # Create a JSON filename based on the XML filename.
        base_name = os.path.basename(xml_file)
        json_filename = os.path.splitext(base_name)[0] + "_summary.json"
        output_path = os.path.join(output_dir, json_filename)
        
        # Write the summary output to a JSON file.
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        
        print(f"Summary for '{xml_file}' has been written to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract XML structure summary from large XML files and write separate JSON files for each.")
    parser.add_argument("--output_dir", type=str, default="xml_summaries",
                        help="Output directory for JSON files (default: xml_summaries)")
    parser.add_argument("--samples", type=int, default=10,
                        help="Number of random samples per tag (default: 10)")
    args = parser.parse_args()
    
    main(args.output_dir, args.samples)
