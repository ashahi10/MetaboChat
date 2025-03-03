#!/usr/bin/env python3

import os
import json
import logging
import time
import xml.etree.ElementTree as ET
from typing import List, Optional, Tuple
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor
import psycopg2

#########################################
# 1) CONFIG & LOGGING
#########################################
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL connection details
DB_NAME = "metabolites_pg"
DB_USER = "postgres"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "5432"

# Paths to the HMDB-style XML files
DATA_FILES = [
    "./data/hmdb_metabolites.xml",
    "./data/feces_metabolites.xml",
    "./data/hmdb_proteins.xml",
    "./data/saliva_metabolites.xml",
    "./data/serum_metabolites.xml",
    "./data/sweat_metabolites.xml",
    "./data/urine_metabolites.xml"
]

#########################################
# 2) DB CONNECTION & SCHEMA CREATION
#########################################
def connect_db():
    """Returns a new psycopg2 connection."""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def create_tables():
    """
    Creates (or verifies) the tables required for storing:
      - metabolites
      - predicted_properties
      - diseases
      - pathways
      - concentrations
      - proteins
    """
    conn = connect_db()
    cur = conn.cursor()

    # 1) Metabolites
    cur.execute('''
        CREATE TABLE IF NOT EXISTS metabolites (
            id SERIAL PRIMARY KEY,
            hmdb_id TEXT UNIQUE NOT NULL,
            name TEXT,
            chemical_formula TEXT,
            synonyms JSONB,
            status TEXT,

            molecular_weight_avg REAL,
            molecular_weight_monoisotopic REAL,
            iupac_name TEXT,
            smiles TEXT,
            inchi TEXT,
            inchikey TEXT,

            taxonomy_kingdom TEXT,
            taxonomy_superclass TEXT,
            taxonomy_class TEXT,
            taxonomy_subclass TEXT,
            taxonomy_direct_parent TEXT,
            taxonomy_alternative_parents JSONB,

            cellular_locations JSONB,
            biospecimen_locations JSONB,
            tissue_locations JSONB,

            creation_date TIMESTAMP NULL,
            update_date TIMESTAMP NULL,
            version TEXT
        );
    ''')

    # 2) Predicted Properties (logP, pKa, etc.)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS predicted_properties (
            id SERIAL PRIMARY KEY,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE CASCADE,
            property_kind TEXT NOT NULL,   -- e.g. "logp", "pka_strongest_acidic"
            property_value TEXT,
            property_source TEXT,
            UNIQUE (metabolite_id, property_kind, property_source)
        );
    ''')

    # 3) Diseases
    cur.execute('''
        CREATE TABLE IF NOT EXISTS diseases (
            id SERIAL PRIMARY KEY,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE CASCADE,
            disease_name TEXT NOT NULL,
            "references" TEXT,
            UNIQUE (metabolite_id, disease_name)
        );
    ''')

    # 4) Pathways
    cur.execute('''
        CREATE TABLE IF NOT EXISTS pathways (
            id SERIAL PRIMARY KEY,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE CASCADE,
            pathway_name TEXT NOT NULL,
            kegg_id TEXT,
            smpdb_id TEXT,
            UNIQUE (metabolite_id, pathway_name, kegg_id)
        );
    ''')

    # 5) Concentrations (both normal & abnormal)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS concentrations (
            id SERIAL PRIMARY KEY,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE CASCADE,
            concentration_type TEXT NOT NULL,   -- "normal" or "abnormal"
            biofluid_type TEXT,
            concentration_value TEXT,
            subject_age TEXT,
            subject_sex TEXT,
            subject_condition TEXT
        );
    ''')

    # 6) Proteins
    cur.execute('''
        CREATE TABLE IF NOT EXISTS proteins (
            id SERIAL PRIMARY KEY,
            uniprot_id TEXT UNIQUE NOT NULL,
            protein_name TEXT,
            gene_name TEXT,
            metabolite_id INT REFERENCES metabolites(id) ON DELETE SET NULL
        );
    ''')

    # CREATE INDEXES
    cur.execute('CREATE INDEX IF NOT EXISTS idx_metabolites_hmdb_id ON metabolites(hmdb_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_pathways_metabolite_id ON pathways(metabolite_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_diseases_metabolite_id ON diseases(metabolite_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_proteins_uniprot_id ON proteins(uniprot_id);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_concentrations_metabolite_id ON concentrations(metabolite_id);')

    conn.commit()
    conn.close()
    logger.info("Tables created or verified successfully.")

#########################################
# 3) HELPER FUNCTIONS
#########################################
def safe_text(element: Optional[ET.Element]) -> Optional[str]:
    """Returns stripped text from an XML element or None if missing."""
    if element is not None and element.text:
        return element.text.strip()
    return None

def safe_float(element: Optional[ET.Element]) -> Optional[float]:
    """Converts text from element to float, or None if invalid."""
    txt = safe_text(element)
    if txt:
        try:
            return float(txt)
        except ValueError:
            return None
    return None

def parse_timestamp(element: Optional[ET.Element]):
    """Very naive approach: store date/time or None if missing.
       If your data only has YYYY-MM-DD, you can parse with datetime.
       For simplicity, store as text or parse manually if needed.
    """
    txt = safe_text(element)
    if not txt:
        return None
    # Attempt "YYYY-MM-DD HH:MM:SS" => convert to a standard format
    # In many HMDB files, date might look like "2021-06-28 16:34:04 UTC"
    # We'll do a quick chop and remove " UTC"
    txt = txt.replace(" UTC", "")
    # Alternatively, store raw text:
    return txt  # Or parse with datetime.strptime if you'd like

def extract_list_values(parent: ET.Element, child_tag: str, ns: str) -> List[str]:
    """Return all text from <child_tag> under 'parent' as a list."""
    results = []
    if parent is not None:
        for c in parent.findall(f"{ns}{child_tag}"):
            val = safe_text(c)
            if val:
                results.append(val)
    return results

#########################################
# 4) PARSING & INSERT LOGIC
#########################################
def parse_hmdb_xml(xml_file: str, conn):
    """
    Main parse function for both metabolite & protein XMLs. 
    The logic is similar for each, but we branch if we see <metabolite> or <protein> as top-level.
    """
    if not os.path.exists(xml_file):
        logger.warning(f"File not found: {xml_file}")
        return

    cursor = conn.cursor()
    start = time.time()

    # Batching containers
    metabolite_batch = {}
    predicted_props_batch = []
    disease_batch = {}
    pathway_batch = {}
    concentration_batch = []
    protein_batch = {}

    count_mets = count_props = count_path = count_dis = count_conc = count_prot = 0

    # We’ll store up to X items in memory before batch-inserting
    BATCH_SIZE = 500

    # HMDB namespace
    ns = "{http://www.hmdb.ca}"

    # Create an iterator for the parse
    context = ET.iterparse(xml_file, events=("start", "end"))
    context = iter(context)
    event, root = next(context)  # get the root element

    try:
        for event, elem in context:
            if event == "end" and elem.tag == f"{ns}metabolite":
                # ~~~~~ METABOLITE BLOCK ~~~~~
                accession_el = elem.find(f"{ns}accession")
                hmdb_id = safe_text(accession_el)
                if not hmdb_id:
                    elem.clear()
                    continue

                # Basic fields
                name_el = elem.find(f"{ns}name")
                name = safe_text(name_el)

                formula_el = elem.find(f"{ns}chemical_formula")
                formula = safe_text(formula_el)

                status_el = elem.find(f"{ns}status")
                status = safe_text(status_el)

                avg_el = elem.find(f"{ns}average_molecular_weight")
                avg_w = safe_float(avg_el)

                mono_el = elem.find(f"{ns}monoisotopic_molecular_weight")
                mono_w = safe_float(mono_el)

                iupac_el = elem.find(f"{ns}iupac_name")
                iupac = safe_text(iupac_el)

                smiles_el = elem.find(f"{ns}smiles")
                smiles = safe_text(smiles_el)

                inchi_el = elem.find(f"{ns}inchi")
                inchi = safe_text(inchi_el)

                inchikey_el = elem.find(f"{ns}inchikey")
                inchikey = safe_text(inchikey_el)

                # synonyms
                synonyms_parent = elem.find(f"{ns}synonyms")
                synonyms_list = extract_list_values(synonyms_parent, "synonym", ns)

                # taxonomy
                tax_el = elem.find(f"{ns}taxonomy")
                if tax_el is not None:
                    king_el = tax_el.find(f"{ns}kingdom")
                    sup_el = tax_el.find(f"{ns}super_class") or tax_el.find(f"{ns}superclass")
                    class_el = tax_el.find(f"{ns}class")
                    subcl_el = tax_el.find(f"{ns}subclass")
                    dirpar_el = tax_el.find(f"{ns}direct_parent")

                    # alternative parents
                    alt_parents_el = tax_el.find(f"{ns}alternative_parents")
                    alt_parents_list = extract_list_values(alt_parents_el, "alternative_parent", ns)

                    kingdom = safe_text(king_el)
                    superc = safe_text(sup_el)
                    cls = safe_text(class_el)
                    subcl = safe_text(subcl_el)
                    dpar = safe_text(dirpar_el)
                else:
                    kingdom = superc = cls = subcl = dpar = None
                    alt_parents_list = []

                # Locations
                cell_locs = extract_list_values(elem, "cellular_location", ns)
                bio_locs = extract_list_values(elem, "biospecimen_location", ns)
                tis_locs = extract_list_values(elem, "tissue_location", ns)

                # creation/update date, version
                creation_el = elem.find(f"{ns}creation_date")
                update_el = elem.find(f"{ns}update_date")
                version_el = elem.find(f"{ns}version")

                creation = parse_timestamp(creation_el)
                update = parse_timestamp(update_el)
                version = safe_text(version_el)

                # Build a row for the batch
                row_data = (
                    hmdb_id,
                    name,
                    formula,
                    json.dumps(synonyms_list),
                    status,
                    avg_w,
                    mono_w,
                    iupac,
                    smiles,
                    inchi,
                    inchikey,
                    kingdom,
                    superc,
                    cls,
                    subcl,
                    dpar,
                    json.dumps(alt_parents_list),
                    json.dumps(cell_locs),
                    json.dumps(bio_locs),
                    json.dumps(tis_locs),
                    creation,
                    update,
                    version
                )
                metabolite_batch[hmdb_id] = row_data
                count_mets += 1

                # Predicted properties
                pprops_el = elem.find(f"{ns}predicted_properties")
                if pprops_el is not None:
                    for prop_el in pprops_el.findall(f"{ns}property"):
                        kind_el = prop_el.find(f"{ns}kind")
                        source_el = prop_el.find(f"{ns}source")
                        val_el = prop_el.find(f"{ns}value")

                        kind = safe_text(kind_el)
                        source = safe_text(source_el) or "Unknown"
                        val = safe_text(val_el)

                        if kind:
                            predicted_props_batch.append((hmdb_id, kind.lower(), val, source))
                            count_props += 1

                # Pathways
                pwy_root = elem.find(f"{ns}biological_properties/{ns}pathways")
                if pwy_root is not None:
                    for pwy in pwy_root.findall(f"{ns}pathway"):
                        name_el = pwy.find(f"{ns}name")
                        kegg_el = pwy.find(f"{ns}kegg_map_id")
                        smpdb_el = pwy.find(f"{ns}smpdb_id")

                        pwy_name = safe_text(name_el)
                        kegg_id = safe_text(kegg_el)
                        smpdb_id = safe_text(smpdb_el)

                        if pwy_name:
                            # Store in your dictionary or batch
                            pathway_batch[(hmdb_id, pwy_name, kegg_id)] = (hmdb_id, pwy_name, kegg_id, smpdb_id)
                            count_path += 1

                # Diseases
                dis_root = elem.find(f"{ns}diseases")
                if dis_root is not None:
                    for disease_el in dis_root.findall(f"{ns}disease"):
                        dname_el = disease_el.find(f"{ns}name")
                        ref_el = disease_el.find(f"{ns}references")
                        dname = safe_text(dname_el)
                        refs = safe_text(ref_el)
                        if dname:
                            disease_batch[(hmdb_id, dname)] = (hmdb_id, dname, refs)
                            count_dis += 1

                # Concentrations
                def parse_concs(section_tag: str, ctype: str):
                    csec = elem.find(f"{ns}{section_tag}")
                    if csec is not None:
                        for con_el in csec.findall(f"{ns}concentration"):
                            fluid_el = con_el.find(f"{ns}biospecimen")
                            cval_el = con_el.find(f"{ns}concentration_value")
                            age_el = con_el.find(f"{ns}subject_age")
                            sex_el = con_el.find(f"{ns}subject_sex")
                            cond_el = con_el.find(f"{ns}subject_condition")

                            fluid = safe_text(fluid_el)
                            cval = safe_text(cval_el)
                            age = safe_text(age_el)
                            sex = safe_text(sex_el)
                            cond = safe_text(cond_el)

                            # We'll store them even if only fluid or cval is present
                            concentration_batch.append((hmdb_id, ctype, fluid, cval, age, sex, cond))

                parse_concs("normal_concentrations", "normal")
                parse_concs("abnormal_concentrations", "abnormal")
                count_conc += len(elem.findall(f"{ns}normal_concentrations/{ns}concentration"))
                count_conc += len(elem.findall(f"{ns}abnormal_concentrations/{ns}concentration"))

                # Clear to free memory
                elem.clear()

            elif event == "end" and elem.tag == f"{ns}protein":
                # ~~~~~ PROTEIN BLOCK ~~~~~
                uniprot_el = elem.find(f"{ns}uniprot_id")
                uniprot_id = safe_text(uniprot_el)
                if not uniprot_id:
                    elem.clear()
                    continue

                protein_name_el = elem.find(f"{ns}name")
                gene_name_el = elem.find(f"{ns}gene_name")

                p_name = safe_text(protein_name_el)
                g_name = safe_text(gene_name_el)

                # optional link to first associated metabolite
                met_assoc_root = elem.find(f"{ns}metabolite_associations")
                link_hmdb_id = None
                if met_assoc_root is not None:
                    first_met = met_assoc_root.find(f"{ns}metabolite")
                    if first_met is not None:
                        acc_el = first_met.find(f"{ns}accession")
                        link_hmdb_id = safe_text(acc_el)

                protein_batch[uniprot_id] = (uniprot_id, p_name, g_name, link_hmdb_id)
                count_prot += 1

                # done
                elem.clear()

            # BATCH INSERT if threshold reached
            if len(metabolite_batch) >= BATCH_SIZE:
                insert_metabolites(metabolite_batch, cursor)
                metabolite_batch.clear()
                conn.commit()

            if len(predicted_props_batch) >= BATCH_SIZE:
                # must ensure metabolites are inserted first
                insert_metabolites(metabolite_batch, cursor)
                metabolite_batch.clear()
                conn.commit()

                insert_predicted_props(predicted_props_batch, cursor)
                predicted_props_batch.clear()
                conn.commit()

            if len(pathway_batch) >= BATCH_SIZE:
                insert_metabolites(metabolite_batch, cursor)
                metabolite_batch.clear()
                conn.commit()

                insert_pathways(pathway_batch, cursor)
                pathway_batch.clear()
                conn.commit()

            if len(disease_batch) >= BATCH_SIZE:
                insert_metabolites(metabolite_batch, cursor)
                metabolite_batch.clear()
                conn.commit()

                insert_diseases(disease_batch, cursor)
                disease_batch.clear()
                conn.commit()

            if len(concentration_batch) >= BATCH_SIZE:
                insert_metabolites(metabolite_batch, cursor)
                metabolite_batch.clear()
                conn.commit()

                insert_concentrations(concentration_batch, cursor)
                concentration_batch.clear()
                conn.commit()

            if len(protein_batch) >= BATCH_SIZE:
                insert_metabolites(metabolite_batch, cursor)
                metabolite_batch.clear()
                conn.commit()

                insert_proteins(protein_batch, cursor)
                protein_batch.clear()
                conn.commit()

        ##############################
        # FINAL FLUSH of any leftover
        ##############################
        insert_metabolites(metabolite_batch, cursor)
        metabolite_batch.clear()
        conn.commit()

        insert_predicted_props(predicted_props_batch, cursor)
        predicted_props_batch.clear()
        conn.commit()

        insert_pathways(pathway_batch, cursor)
        pathway_batch.clear()
        conn.commit()

        insert_diseases(disease_batch, cursor)
        disease_batch.clear()
        conn.commit()

        insert_concentrations(concentration_batch, cursor)
        concentration_batch.clear()
        conn.commit()

        insert_proteins(protein_batch, cursor)
        protein_batch.clear()
        conn.commit()

        elapsed = time.time() - start
        logger.info(
            f"{xml_file} => Metabolites: {count_mets}, PredictedProps: {count_props}, "
            f"Pathways: {count_path}, Diseases: {count_dis}, Concentrations: {count_conc}, "
            f"Proteins: {count_prot} in {elapsed:.2f} s."
        )
    except ET.ParseError as e:
        logger.error(f"XML parse error in {xml_file}: {e}")
        conn.rollback()
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        root.clear()

#########################################
# 5) BATCH INSERT FUNCTIONS
#########################################
def insert_metabolites(metabolite_dict, cursor):
    """Insert or update metabolite data from the batch dict {hmdb_id: row}."""
    if not metabolite_dict:
        return
    rows = list(metabolite_dict.values())
    execute_values(cursor, '''
        INSERT INTO metabolites
        (hmdb_id, name, chemical_formula, synonyms, status,
         molecular_weight_avg, molecular_weight_monoisotopic,
         iupac_name, smiles, inchi, inchikey,
         taxonomy_kingdom, taxonomy_superclass, taxonomy_class, taxonomy_subclass,
         taxonomy_direct_parent, taxonomy_alternative_parents,
         cellular_locations, biospecimen_locations, tissue_locations,
         creation_date, update_date, version)
        VALUES %s
        ON CONFLICT (hmdb_id)
        DO UPDATE SET
          name = EXCLUDED.name,
          chemical_formula = EXCLUDED.chemical_formula,
          synonyms = EXCLUDED.synonyms,
          status = EXCLUDED.status,
          molecular_weight_avg = EXCLUDED.molecular_weight_avg,
          molecular_weight_monoisotopic = EXCLUDED.molecular_weight_monoisotopic,
          iupac_name = EXCLUDED.iupac_name,
          smiles = EXCLUDED.smiles,
          inchi = EXCLUDED.inchi,
          inchikey = EXCLUDED.inchikey,
          taxonomy_kingdom = EXCLUDED.taxonomy_kingdom,
          taxonomy_superclass = EXCLUDED.taxonomy_superclass,
          taxonomy_class = EXCLUDED.taxonomy_class,
          taxonomy_subclass = EXCLUDED.taxonomy_subclass,
          taxonomy_direct_parent = EXCLUDED.taxonomy_direct_parent,
          taxonomy_alternative_parents = EXCLUDED.taxonomy_alternative_parents,
          cellular_locations = EXCLUDED.cellular_locations,
          biospecimen_locations = EXCLUDED.biospecimen_locations,
          tissue_locations = EXCLUDED.tissue_locations,
          creation_date = EXCLUDED.creation_date,
          update_date = EXCLUDED.update_date,
          version = EXCLUDED.version
    ''', rows)

def insert_predicted_props(prop_list, cursor):
    """Insert predicted properties. We must link them to the metabolite_id from 'metabolites' first."""
    if not prop_list:
        return
    # We'll group by hmdb_id so we only do one DB lookup per ID
    # Then build final inserts with the actual metabolite_id
    from collections import defaultdict
    temp_map = defaultdict(list)
    for (hmdb_id, kind, val, source) in prop_list:
        temp_map[hmdb_id].append((kind, val, source))

    # Lookup and build final row
    final_inserts = []
    for hid, items in temp_map.items():
        cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hid,))
        row = cursor.fetchone()
        if row:
            met_id = row[0]
            for (kind, val, source) in items:
                final_inserts.append((met_id, kind, val, source))
    if not final_inserts:
        return

    execute_values(cursor, '''
        INSERT INTO predicted_properties
        (metabolite_id, property_kind, property_value, property_source)
        VALUES %s
        ON CONFLICT (metabolite_id, property_kind, property_source)
        DO UPDATE SET
          property_value = EXCLUDED.property_value
    ''', final_inserts)

def insert_pathways(pathway_dict, cursor):
    """Insert pathway data: must map hmdb_id -> metabolite_id first."""
    if not pathway_dict:
        return
    from collections import defaultdict
    map_hmdb = defaultdict(list)
    for key, val in pathway_dict.items():
        # val = (hmdb_id, pwy_name, kegg, smpdb)
        hmdb_id, pwy_name, kegg_id, smpdb_id = val
        map_hmdb[hmdb_id].append((pwy_name, kegg_id, smpdb_id))

    final_inserts = []
    for hid, items in map_hmdb.items():
        cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hid,))
        row = cursor.fetchone()
        if row:
            mid = row[0]
            for (pwy_name, kegg_id, smpdb_id) in items:
                final_inserts.append((mid, pwy_name, kegg_id, smpdb_id))
    if not final_inserts:
        return

    execute_values(cursor, '''
        INSERT INTO pathways
        (metabolite_id, pathway_name, kegg_id, smpdb_id)
        VALUES %s
        ON CONFLICT (metabolite_id, pathway_name, kegg_id)
        DO UPDATE SET
          smpdb_id = EXCLUDED.smpdb_id
    ''', final_inserts)

def insert_diseases(disease_dict, cursor):
    """Insert disease data: map hmdb_id -> metabolite_id first."""
    if not disease_dict:
        return
    from collections import defaultdict
    map_hmdb = defaultdict(list)
    for key, val in disease_dict.items():
        # val = (hmdb_id, disease_name, references)
        hmdb_id, dname, refs = val
        map_hmdb[hmdb_id].append((dname, refs))

    final_inserts = []
    for hid, items in map_hmdb.items():
        cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hid,))
        row = cursor.fetchone()
        if row:
            mid = row[0]
            for (dname, refs) in items:
                final_inserts.append((mid, dname, refs))
    if not final_inserts:
        return

    execute_values(cursor, '''
        INSERT INTO diseases
        (metabolite_id, disease_name, "references")
        VALUES %s
        ON CONFLICT (metabolite_id, disease_name)
        DO UPDATE SET
          "references" = EXCLUDED."references"
    ''', final_inserts)

def insert_concentrations(conc_list, cursor):
    """
    Insert normal/abnormal concentration data:
      (hmdb_id, "normal"/"abnormal", fluid, value, age, sex, cond)
    Map hmdb_id -> metabolite_id.
    """
    if not conc_list:
        return
    from collections import defaultdict
    map_hmdb = defaultdict(list)
    for c in conc_list:
        hmdb_id, ctype, fluid, cval, age, sex, cond = c
        map_hmdb[hmdb_id].append((ctype, fluid, cval, age, sex, cond))

    final_inserts = []
    for hid, items in map_hmdb.items():
        cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (hid,))
        row = cursor.fetchone()
        if row:
            mid = row[0]
            for (ctype, fluid, cval, age, sex, cond) in items:
                final_inserts.append((mid, ctype, fluid, cval, age, sex, cond))
    if not final_inserts:
        return

    execute_values(cursor, '''
        INSERT INTO concentrations
        (metabolite_id, concentration_type, biofluid_type, concentration_value,
         subject_age, subject_sex, subject_condition)
        VALUES %s
    ''', final_inserts)

def insert_proteins(prot_dict, cursor):
    """
    Insert or update proteins:
      (uniprot_id, protein_name, gene_name, hmdb_id)
    Then map that hmdb_id -> metabolite_id if possible.
    """
    if not prot_dict:
        return
    final_inserts = []
    for upid, val in prot_dict.items():
        (uniprot_id, pname, gname, link_hmdb) = val
        if link_hmdb:
            cursor.execute("SELECT id FROM metabolites WHERE hmdb_id = %s", (link_hmdb,))
            row = cursor.fetchone()
            if row:
                final_inserts.append((uniprot_id, pname, gname, row[0]))
            else:
                # No linked metabolite found
                final_inserts.append((uniprot_id, pname, gname, None))
        else:
            final_inserts.append((uniprot_id, pname, gname, None))

    execute_values(cursor, '''
        INSERT INTO proteins
        (uniprot_id, protein_name, gene_name, metabolite_id)
        VALUES %s
        ON CONFLICT (uniprot_id)
        DO UPDATE SET
            protein_name = EXCLUDED.protein_name,
            gene_name = EXCLUDED.gene_name,
            metabolite_id = EXCLUDED.metabolite_id\
    ''', final_inserts)

#########################################
# 6) MAIN RUN
#########################################
if __name__ == "__main__":
    logger.info("Creating tables (if needed)...")
    create_tables()

    conn = connect_db()
    try:
        # Process each file in parallel, up to some limit
        max_workers = min(4, len(DATA_FILES))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(lambda fp: parse_hmdb_xml(fp, conn), DATA_FILES)
    finally:
        conn.close()
        logger.info("All XML files processed successfully!")
