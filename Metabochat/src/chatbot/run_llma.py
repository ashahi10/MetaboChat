from huggingface_hub import InferenceClient
import sys
import os
import re


# Add the `src` directory to Python's module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from utils.query_database import DatabaseHandler  # Now it should work


# Load your Hugging Face API token from the environment variable
hf_token = os.getenv("HF_TOKEN")
client = InferenceClient(token=hf_token)

# Model ID for Falcon-7B-Instruct
model_id = "tiiuae/falcon-7b-instruct"

# Initialize the database handler
db_handler = DatabaseHandler()

def extract_keywords(prompt):
    """
    Extract potential keywords or phrases from the user's input.
    
    Args:
        prompt (str): The user's input.
    
    Returns:
        dict: A dictionary with possible database fields and corresponding keywords.
    """
    keywords = {}
    
    # Look for specific patterns in the input
    if re.search(r'\bname\b', prompt, re.IGNORECASE):
        keywords['name'] = re.search(r'name\s*[:\- ]*(\w+)', prompt, re.IGNORECASE).group(1)

    if re.search(r'\bdisease\b', prompt, re.IGNORECASE):
        keywords['disease'] = re.search(r'disease\s*[:\- ]*(\w+)', prompt, re.IGNORECASE).group(1)
    
    if re.search(r'\bpathway\b', prompt, re.IGNORECASE):
        keywords['pathway'] = re.search(r'pathway\s*[:\- ]*(\w+)', prompt, re.IGNORECASE).group(1)

    # Generic search terms for broader queries
    search_terms = re.findall(r'\b(?:related to|about|regarding)\s+([\w\s]+)', prompt, re.IGNORECASE)
    if search_terms:
        keywords['search'] = search_terms[0]
    
    return keywords

def query_database(prompt):
    """
    Analyze the user prompt, extract keywords, and query the database if relevant.
    
    Args:
        prompt (str): The user's input.

    Returns:
        str: The database query result or None if no database query is performed.
    """
    keywords = extract_keywords(prompt)
    
    if 'name' in keywords:
        results = db_handler.query_by_name(keywords['name'])
        return format_results(results, ["Name", "Short Description", "Diseases"])
    
    if 'disease' in keywords:
        results = db_handler.query_by_disease(keywords['disease'])
        return format_results(results, ["Name", "Short Description"])
    
    if 'pathway' in keywords:
        results = db_handler.query_by_pathway(keywords['pathway'])
        return format_results(results, ["Name", "Short Description"])
    
    if 'search' in keywords:
        results = db_handler.full_text_search(keywords['search'])
        return format_results(results, ["Name", "Description", "Diseases", "Pathways"])
    
    return None

def format_results(results, headers):
    """
    Format the database query results for display.

    Args:
        results (list): List of tuples representing database rows.
        headers (list): List of column names corresponding to the results.

    Returns:
        str: Formatted string of results.
    """
    if not results:
        return "No results found."
    
    formatted = []
    for row in results:
        row_dict = {headers[i]: value for i, value in enumerate(row)}
        formatted.append("\n".join([f"{key}: {value}" for key, value in row_dict.items()]))
        formatted.append("-" * 50)
    return "\n".join(formatted)

def generate_response(prompt):
    """
    Generates a response using the Falcon-7B-Instruct model via the Hugging Face Inference API.

    Args:
        prompt (str): The input prompt/question to be processed by the model.

    Returns:
        str: The generated response from the model or an error message.
    """
    try:
        print("Generating response...")
        response = client.text_generation(prompt, model=model_id, max_new_tokens=256)

        if isinstance(response, str):
            return response

        if isinstance(response, list) and len(response) > 0:
            if "generated_text" in response[0]:
                return response[0]["generated_text"]
            else:
                return "No 'generated_text' found in response."

        return "Unexpected response format."

    except Exception as e:
        return f"An error occurred: {e}"

def main():
    """
    Main function to take user input and generate responses from the model or database.
    """
    print("Welcome to MetaboChat!")
    print("Ask questions naturally, and the system will query the database or generate a response.")

    while True:
        prompt = input("\nEnter your prompt (or type 'exit' to quit): ")
        if prompt.lower() == "exit":
            print("Exiting MetaboChat. Goodbye!")
            break

        # Check if the prompt matches database queries
        db_response = query_database(prompt)
        if db_response:
            print("\n[Database Response]:")
            print(db_response)
        else:
            # Generate a response from the model
            model_response = generate_response(prompt)
            print("\n[LLM Response]:")
            print(model_response)

if __name__ == "__main__":
    main()
