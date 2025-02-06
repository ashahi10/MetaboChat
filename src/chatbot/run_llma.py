import time
start = time.time()

import openai
import sys
import os
import re

end = time.time()
print(f"Import time: {end - start:.4f} seconds")

# Add the `src` directory to Python's module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from utils.query_database import DatabaseHandler  # Now it should work

# Initialize the Groq API client for OpenAI-compatible calls
client = openai.OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=os.environ.get("groq"),
)

# Model ID for DeepSeek (via Groq)
model_id = "deepseek-r1-distill-llama-70b"

# Initialize the database handler
db_handler = DatabaseHandler()

def extract_keywords(prompt):
    """
    Extracts potential keywords or phrases from the user's input for optimized database searching.
    """
    keywords = {}

    name_match = re.search(r'name\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    disease_match = re.search(r'disease\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    pathway_match = re.search(r'(\b\w+lysis\b|\b\w+genesis\b|\b\w+cycle\b|\b\w+pathway\b)', prompt, re.IGNORECASE)

    if name_match:
        keywords['name'] = name_match.group(1).strip()
    if disease_match:
        keywords['disease'] = disease_match.group(1).strip()
    if pathway_match:
        keywords['pathway'] = pathway_match.group(0).strip()

    search_terms = re.findall(r'\b(?:related to|about|regarding|linked to|involved in|causes)\s+([\w\s]+)', prompt, re.IGNORECASE)
    if search_terms:
        keywords['search'] = search_terms[0].strip()

    return keywords

def query_database(prompt):
    """
    Queries the database based on extracted keywords.
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

def format_results(results, headers, max_items=10):
    """
    Formats the database query results for better readability using bullet points.
    Limits long lists (diseases, pathways, metabolites) to improve clarity.
    """
    if not results:
        return "No relevant database entries found."

    formatted = []
    for row in results:
        row_dict = {headers[i]: str(value).encode('utf-8', 'ignore').decode('utf-8') for i, value in enumerate(row)}

        # Limit long lists in diseases & pathways
        for key in ["Diseases", "Pathways"]:
            if key in row_dict:
                items = row_dict[key].split(", ")
                if len(items) > max_items:
                    row_dict[key] = ", ".join(items[:max_items]) + ", [...] (More omitted)"

        formatted_row = "\n".join([f"- {key}: {value}" for key, value in row_dict.items()])
        formatted.append(formatted_row)
        formatted.append("")  # Blank line between rows

    return "\n".join(formatted)


def clean_response(response_text):
    """
    Cleans up LLM output by:
    - Keeping structured formatting (bullets, bold headings).
    - Removing duplicate new lines.
    - Fixing any weird encoding issues.
    """
    cleaned = response_text.encode('utf-8', 'ignore').decode('utf-8')  # Fix Unicode errors

    # Remove excessive new lines & extra spaces
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
    
    return cleaned


def generate_response(prompt):
    """
    Generates a response using the DeepSeek model via Groq API.
    """
    try:
        if not prompt.strip():
            return "Error: No valid input provided."

        print("Generating response...")
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": (
                    "You are an expert scientific assistant. Provide only direct, concise, and well-organized answers based solely on the provided database information. "
                    "Do NOT include any internal chain-of-thought or reasoning markers. Avoid repetition and do not hallucinate details. "
                    "When comparing data, use a clear table format."
                )},
                {"role": "user", "content": prompt}
            ],
            model=model_id,
            max_tokens=512,  # Reduced for conciseness
            temperature=0.5,  # Moderate randomness
        )

        final_response = response.choices[0].message.content.strip()
        final_response = final_response.encode('utf-8').decode('utf-8')
        return clean_response(final_response)

    except Exception as e:
        return f"Error generating response: {e}"

def synthesize_response(user_prompt, db_response):
    """
    Synthesizes a final answer using both database knowledge and LLM-generated insights.
    Ensures LLM provides useful context if no database match.
    Enforces a strict table format for comparison queries.
    """
    if not db_response or db_response.strip() == "No relevant database entries found.":
        return generate_response(f"The database has no direct match for '{user_prompt}'. "
                                 "However, provide a scientific explanation related to the topic.")

    # Check if user is requesting a comparison (to enforce table format)
    comparative = re.search(r'\b(compare|vs\.?|versus|differences|similarities|contrast)\b', user_prompt, re.IGNORECASE)
    if comparative:
        extra_instructions = (
            "Provide a side-by-side table comparison using ONLY the provided database data. "
            "Ensure key differences and similarities are clearly shown. "
            "Use proper column headers and structured formatting."
        )
    else:
        extra_instructions = (
            "Summarize database data in bullet points (max 150 words). "
            "If data is insufficient, state that clearly."
        )

    enriched_prompt = f"User Query: {user_prompt}\n\nDatabase Information:\n{db_response}\n\nInstructions: {extra_instructions}"
    return generate_response(enriched_prompt)


def main():
    """
    Main function for user interaction and response generation.
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

        db_response, raw_results, headers = query_database(prompt)

        if db_response and db_response != "No relevant database entries found.":
            print("\n[Database Knowledge]:")
            print(db_response)
            final_answer = synthesize_response(prompt, db_response)
            print("\n[Final Answer]:")
            print(final_answer)
        else:
            print("\n[LLM Response]:")
            print(generate_response(prompt))

if __name__ == "__main__":
    main()
