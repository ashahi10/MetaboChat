#!/usr/bin/env python3

import time
start = time.time()

import openai
import sys
import os
import re
import requests

end = time.time()
print(f"Import time: {end - start:.4f} seconds")

# Ensure we can import "query_database.py"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from utils.query_database import PostgresDBHandler

client = openai.OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=os.environ.get("groq"),
)

model_id = "deepseek-r1-distill-llama-70b"

try:
    db_handler = PostgresDBHandler(
        dbname="metabolites_pg",
        user="postgres",
        password="your_password",  # Replace with your actual password
        host="localhost",
        port="5432"
    )
except Exception as e:
    print(f"Error: Failed to connect to database: {e}")
    sys.exit(1)

def extract_keywords(prompt):
    """Extract keywords from the user prompt."""
    keywords = {}
    name_match = re.search(r'(?:molecular weight|structure|role of|what is|of|for|in|with|to|by|hmdb id)\s+([\w\s-]+?)(?:\s+(in|of|with|to|for|by|$))', prompt, re.IGNORECASE)
    disease_match = re.search(r'(?:disease|disorder|condition)\s*[:\- ]*([\w\s]+)', prompt, re.IGNORECASE)
    pathway_match = re.search(r'\b(?:pathway|cycle|metabolism|process)\s+(?:of\s+)?([\w\s]+)', prompt, re.IGNORECASE)
    hmdb_id_match = re.search(r'(HMDB0+\d+)', prompt, re.IGNORECASE)
    biofluid_match = re.search(r'(urine|blood|plasma|csf|cerebrospinal fluid|serum|saliva|feces|sweat)', prompt, re.IGNORECASE)

    if name_match:
        keywords['name'] = name_match.group(1).strip()
    if disease_match:
        keywords['disease'] = disease_match.group(1).strip()
    if pathway_match:
        keywords['pathway'] = pathway_match.group(1).strip()
    if hmdb_id_match:
        keywords['hmdb_id'] = hmdb_id_match.group(1).strip()
    if biofluid_match:
        keywords['biofluid'] = biofluid_match.group(0).strip()

    return keywords

def query_database(prompt):
    """Query the database based on extracted keywords."""
    keys = extract_keywords(prompt)
    try:
        if 'name' in keys:
            by_name = db_handler.query_by_name(keys['name'], limit=5)
            if by_name:
                if "hmdb id" in prompt.lower():
                    return format_results([by_name[0]], ["ID", "HMDB_ID", "Name"]), [by_name[0]], ["ID", "HMDB_ID", "Name"]
                return format_results(by_name, ["ID", "HMDB_ID", "Name", "Formula", "Molecular Weight", "SMILES"]), by_name, ["ID", "HMDB_ID", "Name", "Formula", "Molecular Weight", "SMILES"]
            else:
                fts_rows = db_handler.full_text_search(keys['name'], limit=5)
                return format_results(fts_rows, ["ID", "HMDB_ID", "Name", "Rank"]), fts_rows, ["ID", "HMDB_ID", "Name", "Rank"]

        if 'hmdb_id' in keys:
            row = db_handler.query_by_hmdb_id(keys['hmdb_id'])
            if row:
                headers = ["ID", "HMDB_ID", "Name", "Formula", "Molecular Weight", "SMILES"]
                rows = [row]
                if 'protein' in prompt.lower() or 'proteins' in prompt.lower():
                    proteins = db_handler.query_proteins(keys['hmdb_id'])
                    if proteins:
                        headers.extend(["UniProt ID", "Protein Name", "Gene Name"])
                        rows = [(r + p) for r in rows for p in proteins]
                if 'concentration' in prompt.lower() or 'concentrations' in prompt.lower():
                    ctype = 'abnormal' if 'abnormal' in prompt.lower() else 'normal'
                    biofluid = keys.get('biofluid')
                    concs = db_handler.query_concentrations(keys['hmdb_id'], ctype, biofluid)
                    if concs:
                        headers.extend(["Concentration Type", "Biofluid", "Value", "Age", "Sex", "Condition"])
                        rows = [(r + c) for r in rows for c in concs]
                return format_results(rows, headers), rows, headers
            else:
                fts = db_handler.full_text_search(keys['hmdb_id'], limit=5)
                return format_results(fts, ["ID", "HMDB_ID", "Name", "Rank"]), fts, ["ID", "HMDB_ID", "Name", "Rank"]

        if 'disease' in keys:
            drows = db_handler.query_by_disease(keys['disease'], limit=5)
            if drows:
                return format_results(drows, ["ID", "HMDB_ID", "Name", "Disease"]), drows, ["ID", "HMDB_ID", "Name", "Disease"]

        if 'pathway' in keys:
            prows = db_handler.query_by_pathway(keys['pathway'], limit=5)
            if prows:
                return format_results(prows, ["ID", "HMDB_ID", "Name", "Pathway"]), prows, ["ID", "HMDB_ID", "Name", "Pathway"]

        if 'biofluid' in keys:
            brows = db_handler.query_by_biofluid(keys['biofluid'], limit=5)
            if brows:
                return format_results(brows, ["ID", "HMDB_ID", "Name", "Biofluid"]), brows, ["ID", "HMDB_ID", "Name", "Biofluid"]

        frows = db_handler.full_text_search(prompt, limit=5)
        if frows:
            return format_results(frows, ["ID", "HMDB_ID", "Name", "Rank"]), frows, ["ID", "HMDB_ID", "Name", "Rank"]

        return "No relevant database entries found.", None, None
    except Exception as e:
        print(f"Database query error: {e}")
        return "Error querying database.", None, None

def format_results(rows, headers, max_items=10):
    """Format database results into a readable string."""
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

def clean_response(response_text):
    """Clean the LLM response for proper formatting."""
    cleaned = response_text.encode('utf-8', 'ignore').decode('utf-8')
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    if not cleaned.endswith(('.', '!', '?')) and not cleaned.endswith('```'):
        cleaned += "."
    return cleaned

def generate_response(prompt, is_comparison=False, is_list_based=False, is_simple=False):
    """Generate a smart LLM response with timeout and error handling."""
    try:
        if not prompt.strip():
            return "Error: No valid input provided."

        system_content = (
            "You are a knowledgeable scientific assistant specializing in metabolomics. "
            "Provide answers that start with a general overview suitable for users with basic knowledge, "
            "followed by specific details for those with a scientific background. "
            "Use provided database information if available, supplemented by your expertise. "
            "Ensure responses are accurate, thorough, and formatted in markdown. "
            "Avoid overly complex jargon in the general section, and provide clear explanations."
        )
        if is_simple:
            system_content += (
                " For simple queries (e.g., molecular weight, structure, HMDB ID), provide a concise, direct answer "
                "with minimal elaboration unless requested."
            )
        elif is_list_based:
            system_content += (
                " For list-based queries (e.g., byproducts, indicators), format the response as a markdown table "
                "with clear headers. Start with a general description of the list’s purpose, then provide specific entries."
            )
        elif is_comparison:
            system_content += (
                " For comparison queries, format the response as a markdown table with columns for each feature. "
                "Begin with a general comparison overview, then detail specific differences or similarities."
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
            timeout=10
        )
        final_response = response.choices[0].message.content.strip()
        return clean_response(final_response)
    except requests.exceptions.Timeout:
        print("LLM response timed out after 10 seconds.")
        return "Error: Response generation took too long. Please try again later."
    except openai.error.AuthenticationError:
        print("Authentication error: Invalid API key.")
        return "Error: Invalid API key. Please check your Groq API key."
    except openai.error.APIError as e:
        print(f"API error: {e}")
        return f"Error: API error occurred. {e}"
    except Exception as e:
        print(f"Unexpected error: {e}")
        return "Error: An unexpected error occurred while generating the response."

def synthesize_response(user_prompt, db_response):
    """Synthesize a response combining database results and LLM output."""
    simple = re.search(r'\b(molecular weight|structure|role of|what is|hmdb id)\b', user_prompt, re.IGNORECASE) and not re.search(r'\b(compare|list|which|how|altered)\b', user_prompt, re.IGNORECASE)
    comparative = re.search(r'\b(compare|vs\.?|versus|differences|similarities|contrast)\b', user_prompt, re.IGNORECASE)
    list_based = re.search(r'\b(list|key|top|three|several|commonly|which|byproducts|indicators|altered|found in)\b', user_prompt, re.IGNORECASE)

    if not db_response or db_response.strip() in ["No relevant database entries found.", "Error querying database."]:
        fallback_prompt = (
            f"No specific data was found in the database for your query: '{user_prompt}'. "
            "Based on general metabolomics knowledge, provide a response that starts with a broad overview "
            "and then includes specific details. If applicable, suggest why the information might not be available "
            "(e.g., rare metabolite, incomplete database) and offer related insights or alternative queries."
        )
        return generate_response(fallback_prompt, is_comparison=comparative, is_list_based=list_based, is_simple=simple)

    if simple:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Provide a concise, direct answer in markdown format using the database data. "
            "Start with a brief general statement, followed by the specific answer."
        )
        return generate_response(instructions, is_comparison=False, is_list_based=False, is_simple=True)
    elif comparative:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Format the answer as a markdown table comparing relevant features. "
            "Begin with a general overview of the comparison, then provide specific details from the database."
        )
        return generate_response(instructions, is_comparison=True, is_list_based=False, is_simple=False)
    elif list_based:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Format the answer as a markdown table listing the key items with descriptions. "
            "Start with a general explanation of the list’s context, then include specific database entries."
        )
        return generate_response(instructions, is_comparison=False, is_list_based=True, is_simple=False)
    else:
        instructions = (
            f"User Query:\n{user_prompt}\n\n"
            f"Database Results:\n{db_response}\n\n"
            "Provide a thorough explanation in markdown format. "
            "Begin with a general overview, then incorporate all relevant database data and supplement with specific details."
        )
        return generate_response(instructions, is_comparison=False, is_list_based=False, is_simple=False)

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

        db_response, raw_rows, used_headers = query_database(user_q)
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