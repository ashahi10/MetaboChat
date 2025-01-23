from Bio import SeqIO
import pandas as pd

def parse_fasta(file_path, output_file):
    """
    Parse a FASTA file and extract sequence data.
    """
    sequences = []
    for record in SeqIO.parse(file_path, "fasta"):
        sequences.append({
            "ID": record.id,
            "Description": record.description,
            "Sequence": str(record.seq)
        })

    df = pd.DataFrame(sequences)
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    # Parse protein sequences
    parse_fasta("Metabochat/data/protein.fasta", "Metabochat/data/protein_sequences.csv")

    # Parse gene sequences
    parse_fasta("Metabochat/data/gene.fasta", "Metabochat/data/gene_sequences.csv")
