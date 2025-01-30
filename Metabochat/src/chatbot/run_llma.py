from huggingface_hub import InferenceClient
import sys
import os
import re

# Add the `src` directory to Python's module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from utils.query_database import DatabaseHandler  # Now it should work

# Load Hugging Face API token
hf_token = os.getenv("HF_TOKEN")
client = InferenceClient(token=hf_token)

# Model ID for Falcon-7B-Instruct
model_id = "tiiuae/falcon-7b-instruct"

# Initialize the database handler
db_handler = DatabaseHandler()

def extract_keywords(prompt):
    """
    Extracts potential keywords or phrases from the user's input for optimized database searching.

    Args:
        prompt (str): User's input.

    Returns:
        dict: Dictionary containing database search fields and keywords.
    """
    keywords = {}

    # Extract specific search terms dynamically
    name_match = re.search(r'name\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    disease_match = re.search(r'disease\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    pathway_match = re.search(r'(\b\w+lysis\b|\b\w+genesis\b|\b\w+cycle\b|\b\w+pathway\b)', prompt, re.IGNORECASE)

    if name_match:
        keywords['name'] = name_match.group(1).strip()
    if disease_match:
        keywords['disease'] = disease_match.group(1).strip()
    if pathway_match:
        keywords['pathway'] = pathway_match.group(0).strip()

    # Generic search terms (catch-all)
    search_terms = re.findall(r'\b(?:related to|about|regarding|linked to|involved in|causes)\s+([\w\s]+)', prompt, re.IGNORECASE)
    if search_terms:
        keywords['search'] = search_terms[0].strip()

    return keywords

def query_database(prompt):
    """
    Queries the database based on extracted keywords.

    Args:
        prompt (str): User's input.

    Returns:
        tuple: (Formatted query result, raw database results, column headers) or (None, None, None) if no results.
    """
    keywords = extract_keywords(prompt)

    if 'name' in keywords:
        results = db_handler.query_by_name(keywords['name'])
        return format_results(results, ["Name", "Short Description", "Diseases"]), results, ["Name", "Short Description", "Diseases"]

    if 'disease' in keywords:
        results = db_handler.query_by_disease(keywords['disease'])
        return format_results(results, ["Name", "Short Description"]), results, ["Name", "Short Description"]

    if 'pathway' in keywords:
        results = db_handler.query_by_pathway(keywords['pathway'])
        return format_results(results, ["Name", "Short Description"]), results, ["Name", "Short Description"]

    if 'search' in keywords:
        results = db_handler.full_text_search(keywords['search'])
        return format_results(results, ["Name", "Description", "Diseases", "Pathways"]), results, ["Name", "Description", "Diseases", "Pathways"]

    return None, None, None

def format_results(results, headers):
    """
    Formats the database query results for display.

    Args:
        results (list): List of tuples representing database rows.
        headers (list): Column names corresponding to results.

    Returns:
        str: Formatted results or "No results found."
    """
    if not results:
        return "No relevant database entries found."

    formatted = []
    for row in results:
        row_dict = {headers[i]: value for i, value in enumerate(row)}
        formatted.append("\n".join([f"{key}: {value}" for key, value in row_dict.items()]))
        formatted.append("-" * 50)

    return "\n".join(formatted)

def generate_response(prompt):
    """
    Generates a response using Falcon-7B-Instruct model via Hugging Face Inference API.

    Args:
        prompt (str): Input prompt.

    Returns:
        str: Generated response from the model or error message.
    """
    try:
        if not prompt.strip():
            return "Error: No valid input provided."

        print("Generating response...")
        response = client.text_generation(prompt, model=model_id, max_new_tokens=256)

        if isinstance(response, str):
            return response.strip()

        if isinstance(response, list) and response and "generated_text" in response[0]:
            return response[0]["generated_text"].strip()

        return "Error: Unexpected response format."

    except Exception as e:
        return f"Error generating response: {e}"

def synthesize_response(user_prompt, db_response):
    """
    Synthesizes a final response using both database knowledge and LLM-generated insights.

    Args:
        user_prompt (str): Original user input.
        db_response (str): Retrieved database information.

    Returns:
        str: Final synthesized response.
    """
    enriched_prompt = (
        f"The user asked: {user_prompt}\n\n"
        f"Here is the relevant database information:\n{db_response}\n\n"
        f"Based on this data, provide a clear, concise, and informative answer."
    )
    return generate_response(enriched_prompt)

def main():
    """
    Main function to handle user interaction and response generation.
    """
    print("Welcome to MetaboChat!")
    print("Ask questions naturally, and the system will provide responses based on database knowledge and LLM insights.")

    while True:
        prompt = input("\nEnter your query (or type 'exit' to quit): ").strip()

        if not prompt:
            print("\n[Error]: Please enter a valid question.")
            continue

        if prompt.lower() == "exit":
            print("Exiting MetaboChat. Goodbye!")
            break

        # Query database first
        db_response, raw_results, headers = query_database(prompt)

        if db_response:
            print("\n[Database Knowledge]:")
            print(db_response)

            # Use both database results and LLM for a final answer
            final_answer = synthesize_response(prompt, db_response)
            print("\n[Final Answer]:")
            print(final_answer)
        else:
            # No database results, rely on LLM
            print("\n[LLM Response]:")
            print(generate_response(prompt))

if __name__ == "__main__":
    main()
