import xml.etree.ElementTree as ET
from collections import defaultdict

def traverse_element(element, path="", tag_counts=None):
    """
    Recursively traverse an XML element and all its children,
    recording tag paths (e.g., 'metabolite/subtag/...') in a dictionary.
    """
    if tag_counts is None:
        tag_counts = defaultdict(int)
    
    # Build a slash-separated path like "metabolite/subtag/..."
    # We only use the local part of the tag if there's a namespace.
    local_tag = element.tag.split("}")[-1]
    
    # If there's no existing path yet, start with local_tag
    # Otherwise, extend the existing path
    current_path = local_tag if not path else f"{path}/{local_tag}"
    
    # Record that we found this path
    tag_counts[current_path] += 1
    
    # Recursively traverse children
    for child in element:
        traverse_element(child, current_path, tag_counts)
    
    return tag_counts


def analyze_xml_structure(file_path, output_file, max_entries=10):
    """
    Parse up to 'max_entries' (e.g., 10) <metabolite> or <protein> entries from an XML,
    build a dictionary of all unique tag/sub-tag paths encountered, and write stats to a file.
    """
    tag_counts = defaultdict(int)
    context = ET.iterparse(file_path, events=("start", "end"))
    context = iter(context)
    
    # Advance to the root
    event, root = next(context)
    
    found_count = 0
    
    for event, elem in context:
        if event == "end" and (elem.tag.endswith("metabolite") or elem.tag.endswith("protein")):
            # Recursively record this element's structure
            traverse_element(elem, path="", tag_counts=tag_counts)

            found_count += 1
            if found_count >= max_entries:
                break  # Stop parsing once we've processed enough entries
            
            # Free memory by clearing this element (and references) from the tree
            root.clear()
    
    # Sort the paths alphabetically to make it easier to scan
    sorted_paths = sorted(tag_counts.items(), key=lambda x: x[0])
    
    # Write summary to output
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"Analyzed up to {max_entries} entries from {file_path}\n\n")
        f.write("PATH\t\t\tOCCURRENCES\n")
        f.write("--------------------------------------------\n")
        for path, count in sorted_paths:
            f.write(f"{path}\t\t{count}\n")

    print(f"Done analyzing {file_path} - results in {output_file}")


if __name__ == "__main__":
    # Example usage: you can define your XML files here and call analyze_xml_structure
    xml_files = {
        "./data/csf_metabolites.xml": "csf_structure_report.txt",
        "./data/feces_metabolites.xml": "feces_structure_report.txt",
        "./data/hmdb_proteins.xml": "proteins_structure_report.txt",
        "./data/saliva_metabolites.xml": "saliva_structure_report.txt",
        "./data/serum_metabolites.xml": "serum_structure_report.txt",
        "./data/sweat_metabolites.xml": "sweat_structure_report.txt",
        "./data/urine_metabolites.xml": "urine_structure_report.txt",
        # Uncomment if needed:
        "./data/hmdb_metabolites.xml": "hmdb_structure_report.txt",
    }
    
    for file_path, output_file in xml_files.items():
        print(f"Analyzing structure of {file_path}...")
        try:
            analyze_xml_structure(file_path, output_file, max_entries=10)
        except FileNotFoundError:
            print(f"File not found: {file_path}. Skipping...")
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}. Skipping...")
        print("-" * 50)
