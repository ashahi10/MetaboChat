import xml.etree.ElementTree as ET

def inspect_xml(file_path, output_file, sample_size=1):
    """
    Extracts the structure of the first few entries from an XML file
    and saves it to a text file.
    """
    context = ET.iterparse(file_path, events=("start", "end"))
    context = iter(context)
    event, root = next(context)

    with open(output_file, "w", encoding="utf-8") as f:
        count = 0
        for event, elem in context:
            if event == "end" and elem.tag.endswith("metabolite") or elem.tag.endswith("protein"):  # Handle both metabolites and proteins
                f.write(ET.tostring(elem, encoding="unicode"))  # Save XML structure
                f.write("\n\n" + "="*80 + "\n\n")  # Separator between entries

                count += 1
                if count >= sample_size:
                    break  # Stop after extracting 'sample_size' entries
                
                root.clear()  # Free memory

if __name__ == "__main__":
    # Define file paths and corresponding output files
    xml_files = {
        "./data/csf_metabolites.xml": "csf_sample_output.txt",
        "./data/feces_metabolites.xml": "feces_sample_output.txt",
        "./data/hmdb_proteins.xml": "proteins_sample_output.txt",
        "./data/saliva_metabolites.xml": "saliva_sample_output.txt",
        "./data/serum_metabolites.xml": "serum_sample_output.txt",
        "./data/sweat_metabolites.xml": "sweat_sample_output.txt",
        "./data/urine_metabolites.xml": "urine_sample_output.txt",
        # "./data/hmdb_metabolites.xml": "hmdb_sample_output.txt",  # Commented out to avoid overwriting
    }

    # Process each XML file
    for file_path, output_file in xml_files.items():
        print(f"Extracting first entry from {file_path}...")
        try:
            inspect_xml(file_path, output_file, sample_size=1)
            print(f"Sample XML structure saved to {output_file}")
        except FileNotFoundError:
            print(f"Error: {file_path} not found. Skipping...")
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}. Skipping...")
        print("-" * 50)