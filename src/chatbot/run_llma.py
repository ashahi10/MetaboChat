#!/usr/bin/env python3

import time
start = time.time()

import openai
import sys
import os
import re

end = time.time()
print(f"Import time: {end - start:.4f} seconds")

# Ensure we can import "query_database.py"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from utils.query_database import PostgresDBHandler

###############################################
# LLM (Groq) / OpenAI-like Setup
###############################################
client = openai.OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=os.environ.get("groq"),
)

model_id = "deepseek-r1-distill-llama-70b"  # Adjust as needed

###############################################
# Initialize DB Handler with Error Handling
###############################################
try:
    db_handler = PostgresDBHandler(
        dbname="metabolites_pg",
        user="postgres",
        password="your_password",  # Use environment variable in production
        host="localhost",
        port="5432"
    )
except Exception as e:
    print(f"Error: Failed to connect to database: {e}")
    sys.exit(1)

###############################################
# Utility: Extract Keywords
###############################################
def extract_keywords(prompt):
    """
    Enhanced detection of:
      - name
      - disease
      - pathway
      - HMDB ID
      - biofluid
      - 'search'
    Improved to catch broader context.
    """
    keywords = {}

    name_match = re.search(r'(?:name|metabolite|compound)\s*[:\- ]*([\w\s-]+)', prompt, re.IGNORECASE)
    disease_match = re.search(r'(?:disease|disorder)\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    pathway_match = re.search(r'(\b\w+lysis\b|\b\w+genesis\b|\b\w+cycle\b|\b\w+pathway\b)', prompt, re.IGNORECASE)
    hmdb_id_match = re.search(r'(HMDB0+\d+)', prompt, re.IGNORECASE)
    biofluid_match = re.search(r'(urine|blood|plasma|csf|cerebrospinal fluid|serum|saliva|feces|sweat)', prompt, re.IGNORECASE)

    if name_match:
        keywords['name'] = name_match.group(1).strip()
    if disease_match:
        keywords['disease'] = disease_match.group(1).strip()
    if pathway_match:
        keywords['pathway'] = pathway_match.group(0).strip()
    if hmdb_id_match:
        keywords['hmdb_id'] = hmdb_id_match.group(1).strip()
    if biofluid_match:
        keywords['biofluid'] = biofluid_match.group(0).strip()

    search_phrases = re.findall(r'\b(?:related to|about|regarding|linked to|involved in|causes)\s+([\w\s]+)', prompt, re.IGNORECASE)
    if search_phrases:
        keywords['search'] = search_phrases[0].strip()

    return keywords

###############################################
# DB Query
###############################################
def query_database(prompt):
    """
    Enhanced querying with prioritized methods:
      - HMDB ID
      - Name
      - Disease
      - Pathway
      - Biofluid
      - Full-text search as fallback
    Returns (formatted_str, raw_rows, used_headers).
    """
    keys = extract_keywords(prompt)

    try:
        # Check HMDB ID
        if 'hmdb_id' in keys:
            row = db_handler.query_by_hmdb_id(keys['hmdb_id'])
            if row:
                headers = ["ID", "HMDB_ID", "Name", "Formula", "Molecular Weight", "SMILES"]
                rows = [row]
                return format_results(rows, headers), rows, headers
            else:
                fts = db_handler.full_text_search(keys['hmdb_id'], limit=5)
                fts_headers = ["ID", "HMDB_ID", "Name", "Rank"]
                return format_results(fts, fts_headers), fts, fts_headers

        # Check name
        if 'name' in keys:
            by_name = db_handler.query_by_name(keys['name'], limit=5)
            if by_name:
                headers = ["ID", "HMDB_ID", "Name", "Formula", "Molecular Weight", "SMILES"]
                return format_results(by_name, headers), by_name, headers
            else:
                fts_rows = db_handler.full_text_search(keys['name'], limit=5)
                fts_headers = ["ID", "HMDB_ID", "Name", "Rank"]
                return format_results(fts_rows, fts_headers), fts_rows, fts_headers

        # Disease
        if 'disease' in keys:
            drows = db_handler.query_by_disease(keys['disease'], limit=5)
            if drows:
                headers = ["ID", "HMDB_ID", "Name", "Disease"]
                return format_results(drows, headers), drows, headers

        # Pathway
        if 'pathway' in keys:
            prows = db_handler.query_by_pathway(keys['pathway'], limit=5)
            if prows:
                headers = ["ID", "HMDB_ID", "Name", "Pathway"]
                return format_results(prows, headers), prows, headers

        # Biofluid
        if 'biofluid' in keys:
            brows = db_handler.query_by_biofluid(keys['biofluid'], limit=5)
            if brows:
                headers = ["ID", "HMDB_ID", "Name", "Biofluid"]
                return format_results(brows, headers), brows, headers

        # Search or fallback context
        if 'search' in keys:
            term = keys.get('search', '')
            srows = db_handler.full_text_search(term or prompt, limit=5)
            sheaders = ["ID", "HMDB_ID", "Name", "Rank"]
            return format_results(srows, sheaders), srows, sheaders

        # Fallback: Full-text search on entire prompt
        frows = db_handler.full_text_search(prompt, limit=5)
        fheaders = ["ID", "HMDB_ID", "Name", "Rank"]
        if frows:
            return format_results(frows, fheaders), frows, fheaders

        return "No relevant database entries found.", None, None

    except Exception as e:
        print(f"Database query error: {e}")
        return "Error querying database.", None, None

###############################################
# Format DB Results
###############################################
def format_results(rows, headers, max_items=10):
    if not rows:
        return "No relevant database entries found."

    lines = []
    for row in rows[:max_items]:
        row_strs = []
        for i, col in enumerate(headers):
            if i < len(row) and col:
                val = str(row[i]) if row[i] is not None else "N/A"
                row_strs.append(f"- {col}: {val}")
        lines.append("\n".join(row_strs))

    return "\n\n".join(lines)

###############################################
# Clean LLM Output
###############################################
def clean_response(response_text):
    cleaned = response_text.encode('utf-8', 'ignore').decode('utf-8')
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
    # Remove <think> tags and any incomplete reasoning
    cleaned = re.sub(r'<think>.*?</think>', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'<[^>]+>', '', cleaned)  # Remove any lingering tags
    if not cleaned.endswith(('.', '!', '?')) and not cleaned.endswith('```'):
        cleaned += "."
    return cleaned

###############################################
# LLM Generation with Conditional Logic
###############################################
def generate_response(prompt, is_comparison=False, is_list_based=False, is_simple=False):
    """
    Generate LLM response with conditional logic for tables, conciseness, and polished output.
    """
    try:
        if not prompt.strip():
            return "Error: No valid input provided."

        system_content = (
            "You are a knowledgeable scientific assistant specializing in metabolomics. "
            "Use provided database information if available, supplemented by your expertise. "
            "Provide thorough, accurate answers in markdown format. "
            "Do not include internal reasoning (<think> tags) or incomplete sentences in the response."
        )
        if is_simple:
            system_content += (
                " For simple queries (e.g., molecular weight, structure), provide a concise, direct answer without elaboration unless requested."
            )
        elif is_list_based:
            system_content += (
                " For list-based queries (e.g., byproducts, indicators), format the response as a markdown table with clear headers and complete data."
            )
        elif is_comparison:
            system_content += (
                " For comparison queries, format the response as a markdown table with columns for each relevant feature."
            )

        print("Generating LLM response...")
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": prompt}
            ],
            model=model_id,
            max_tokens=2048,
            temperature=0.3,
        )

        final_response = response.choices[0].message.content.strip()
        return clean_response(final_response)

    except Exception as e:
        print(f"LLM error: {e}")
        return f"Error generating response: {e}"

###############################################
# Combine DB + LLM with Query Type Detection
###############################################
def synthesize_response(user_prompt, db_response):
    """
    Integrate database results with LLM for comprehensive answers.
    Detect query type (simple, comparative, list-based) for appropriate formatting.
    """
    # Enhanced query type detection
    simple = re.search(r'\b(molecular weight|structure|role of|what is)\b', user_prompt, re.IGNORECASE) and not re.search(r'\b(compare|list|which|how|altered)\b', user_prompt, re.IGNORECASE)
    comparative = re.search(r'\b(compare|vs\.?|versus|differences|similarities|contrast)\b', user_prompt, re.IGNORECASE)
    list_based = re.search(r'\b(list|key|top|three|several|commonly|which|byproducts|indicators|altered|found in)\b', user_prompt, re.IGNORECASE)

    if not db_response or db_response.strip() in ["No relevant database entries found.", "Error querying database."]:
        fallback_prompt = (
            f"No direct database match was found for your query: '{user_prompt}'. "
            "Provide a detailed, scientifically accurate answer based on your knowledge. "
            "Ensure the response is complete, concise, and formatted clearly in markdown."
        )
        return generate_response(fallback_prompt, is_comparison=comparative, is_list_based=list_based, is_simple=simple)

    if simple:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Provide a concise, direct answer in markdown format using the database data. "
            "Supplement with minimal additional details only if necessary."
        )
        return generate_response(instructions, is_comparison=False, is_list_based=False, is_simple=True)
    elif comparative:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Format the answer as a markdown table with columns for each relevant feature. "
            "Incorporate database data and supplement with additional details where needed."
        )
        return generate_response(instructions, is_comparison=True, is_list_based=False, is_simple=False)
    elif list_based:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Format the answer as a markdown table listing the key items with their descriptions. "
            "Incorporate database data and supplement with additional details where needed."
        )
        return generate_response(instructions, is_comparison=False, is_list_based=True, is_simple=False)
    else:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Provide a thorough, concise explanation in markdown format. "
            "Incorporate all relevant database data and supplement with your knowledge if data is incomplete."
        )
        return generate_response(instructions, is_comparison=False, is_list_based=False, is_simple=False)

###############################################
# Main Loop
###############################################
def main():
    print("Welcome to MetaboChat! Ask any question about metabolites, diseases, or pathways (or type 'exit' to quit).")

    while True:
        user_q = input("\nEnter your query: ").strip()
        if not user_q:
            print("\n[Error]: Please enter a valid question. Try asking about metabolites, diseases, or pathways.")
            continue
        if user_q.lower() == "exit":
            print("Exiting MetaboChat. Goodbye!")
            break

        # Query the database
        db_response, raw_rows, used_headers = query_database(user_q)

        # Process response
        if db_response and db_response not in ["No relevant database entries found.", "Error querying database."]:
            print("\n[Database Results]:")
            print(db_response)
            final_answer = synthesize_response(user_q, db_response)
            print("\n[Final Answer]:")
            print(final_answer)
        else:
            print("\n[No relevant DB data or empty results] => LLM fallback.")
            fallback_answer = synthesize_response(user_q, db_response)
            print("\n[LLM Response]:")
            print(fallback_answer)

if __name__ == "__main__":
    main()