#!/usr/bin/env python3

import time
start = time.time()

import sys
import os
import re
import openai

end = time.time()
print(f"Import time: {end - start:.4f} seconds")

# Ensure we can import our query_database script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

# 1) Use the newly updated PostgresDBHandler from your final query_database.py
from utils.query_database import PostgresDBHandler

###########################################
# LLM (Groq or OpenAI-compatible) Setup
###########################################
# If you're using Groq for the LLM calls, do this:
# client = openai.OpenAI(
#     base_url="https://api.groq.com/openai/v1",
#     api_key=os.environ.get("groq"),
# )

# Or if you use standard OpenAI:
# openai.api_key = os.environ["OPENAI_API_KEY"]
# client = openai

# Example placeholder:
client = openai  # Adjust as needed for your environment

# This is your chosen model (Groq or openai)
model_id = "gpt-3.5-turbo"  # or "deepseek-r1-distill-llama-70b"

###########################################
# Initialize DB Handler
###########################################
db_handler = PostgresDBHandler(
    dbname="metabolites_pg",
    user="postgres",
    password="your_password",
    host="localhost",
    port="5432"
)

###########################################
# UTILS
###########################################
def extract_keywords(prompt):
    """
    Extract potential keywords from the user's prompt:
     - name
     - disease
     - pathway
    If none, we might fallback on full_text_search.
    """
    keywords = {}

    # Basic detection via simple regex
    name_match = re.search(r'name\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    disease_match = re.search(r'disease\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    # Also catch "diabetes" or "cancer" type references
    pathway_match = re.search(r'(glycolysis|gluconeogenesis|\b\w+lysis\b|\b\w+genesis\b|\b\w+cycle\b|\b\w+pathway\b)', 
                              prompt, re.IGNORECASE)

    if name_match:
        keywords['name'] = name_match.group(1).strip()
    if disease_match:
        keywords['disease'] = disease_match.group(1).strip()
    if pathway_match:
        keywords['pathway'] = pathway_match.group(0).strip()

    return keywords

def query_database(prompt):
    """
    Decide how to query the DB based on extracted keywords:
      1) name => query_by_name
      2) disease => query_by_disease
      3) pathway => query_by_pathway
      If none => fallback to full_text_search on the entire prompt
    Returns (formatted_str, raw_rows, columns_used)
    """
    keywords = extract_keywords(prompt)

    # if user specifically gave a name/disease/pathway, we use direct queries
    if 'name' in keywords:
        rows = db_handler.query_by_name(keywords['name'], limit=5)
        if not rows:
            # fallback to FTS on the name
            rows = db_handler.full_text_search(keywords['name'], limit=5)
            return format_results(rows, ["ID", "HMDB_ID", "Name", "Rank" if len(rows) and len(rows[0]) > 3 else None]), rows, ["ID", "HMDB_ID", "Name"]

        return format_results(rows, ["ID", "HMDB_ID", "Name", "Formula"]), rows, ["ID", "HMDB_ID", "Name", "Formula"]

    if 'disease' in keywords:
        rows = db_handler.query_by_disease(keywords['disease'], limit=5)
        return format_results(rows, ["ID", "HMDB_ID", "Name", "Disease"]), rows, ["ID", "HMDB_ID", "Name", "Disease"]

    if 'pathway' in keywords:
        rows = db_handler.query_by_pathway(keywords['pathway'], limit=5)
        return format_results(rows, ["ID", "HMDB_ID", "Name", "Pathway"]), rows, ["ID", "HMDB_ID", "Name", "Pathway"]

    # else: no direct field => fallback full-text across doc
    rows = db_handler.full_text_search(prompt, limit=5)
    if rows:
        # If the row structure is (id, hmdb_id, name, rank), let's label them
        return format_results(rows, ["ID", "HMDB_ID", "Name", "Rank"]), rows, ["ID", "HMDB_ID", "Name", "Rank"]
    return "No relevant database entries found.", None, None

def format_results(rows, headers, max_items=10):
    """
    Format the row data using bullet points, ignoring null fields.
    """
    if not rows:
        return "No relevant database entries found."

    lines = []
    for row in rows:
        row_strs = []
        # tie each row element to the matching header
        for i, h in enumerate(headers):
            if not h:
                continue
            if i < len(row):
                val = str(row[i])
                # e.g. if val is "None", skip or set empty
                if val.lower() == "none":
                    val = ""
                row_strs.append(f"- {h}: {val}")
        lines.append("\n".join(row_strs))
    return "\n\n".join(lines)

def clean_response(response_text):
    """
    Cleans up LLM output:
      - remove weird spacing
      - unify newlines
    """
    cleaned = response_text.encode('utf-8', 'ignore').decode('utf-8')
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
    return cleaned

def generate_response(prompt):
    """
    Generates a response from the LLM. 
    If you're using Groq or openai, adapt accordingly.
    """
    if not prompt.strip():
        return "Error: No valid input provided."

    # Example using openai
    try:
        print("Generating LLM response...")
        response = client.ChatCompletion.create(
            model=model_id,
            messages=[
                {"role": "system", "content": (
                    "You are an expert scientific assistant. Provide direct, concise, well-organized answers based solely on the provided database info. "
                    "Do not show your chain-of-thought. Keep answers short and factual. "
                    "When comparing or enumerating data, use bullet points or a table."
                )},
                {"role": "user", "content": prompt}
            ],
            max_tokens=512,
            temperature=0.3,
        )
        content = response["choices"][0]["message"]["content"].strip()
        return clean_response(content)
    except Exception as e:
        return f"Error generating response: {e}"

def synthesize_response(user_prompt, db_response):
    """
    Combines DB knowledge with LLM summary.
    If db_response is "No relevant database entries found", let LLM respond freely.
    Otherwise, pass the DB data + user query for final summarization.
    """
    if not db_response or db_response.strip() == "No relevant database entries found.":
        # Let LLM generate an answer with no data
        return generate_response(f"No direct database match for '{user_prompt}'. Provide a short scientific explanation.")

    # If there's data, we instruct the LLM to summarize
    # Check if user might want a comparison
    comparative = re.search(r'\b(compare|vs\.?|versus|differences|similarities|contrast)\b', user_prompt, re.IGNORECASE)
    if comparative:
        extra_instructions = (
            "Provide a side-by-side table using only the database data. "
            "Highlight key differences and similarities in each column."
        )
    else:
        extra_instructions = (
            "Summarize the database data in bullet points (max 150 words). "
            "If data is insufficient, say so."
        )

    combined_prompt = f"User Query:\n{user_prompt}\n\nDatabase Results:\n{db_response}\n\nInstructions:\n{extra_instructions}"
    return generate_response(combined_prompt)

#######################################
# MAIN Chat Loop
#######################################
def main():
    print("Welcome to MetaboChat with Weighted FTS!")
    print("Ask a question about a metabolite name, disease, or pathway. Type 'exit' to quit.")

    while True:
        prompt = input("\nEnter your query: ").strip()
        if not prompt:
            print("[Error]: Please enter a valid question.")
            continue

        if prompt.lower() == "exit":
            print("Goodbye!")
            break

        # Query DB
        db_response, raw_results, used_headers = query_database(prompt)

        # If we have data from DB, combine with LLM. Otherwise, free LLM response.
        if db_response and db_response != "No relevant database entries found.":
            print("\n[Database Output]:")
            print(db_response)
            final = synthesize_response(prompt, db_response)
            print("\n[Final Answer]:")
            print(final)
        else:
            print("\n[No direct DB match or empty] => LLM fallback.")
            fallback_resp = generate_response(prompt)
            print("\n[LLM Response]:")
            print(fallback_resp)

if __name__ == "__main__":
    main()
