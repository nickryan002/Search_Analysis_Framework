import csv
import shingles_dict_generator
from sortedcontainers import SortedDict

ENTITY_TABLE_CSV = 'ShingleEntityMatcher/entity_table_new.csv'
SYNONYMS_TXT = 'ShingleEntityMatcher/lulu_solr_synonyms.txt'
SYNONYM_MATCHES_CSV = 'ShingleEntityMatcher/SynonymExpansions.csv'
REWRITTEN_SYNONYMS_TXT = 'ShingleEntityMatcher/synonyms.txt'

# Global SortedDict to store shingles with their corresponding details
shingles_dict = SortedDict()

shingles_dict_generator.read_csv_and_populate_shingles_dict(ENTITY_TABLE_CSV, shingles_dict)

def process_synonyms(shingles_dict: dict) -> None:
    """
    Processes synonyms from a text file, matches them with entries in the shingles dictionary,
    and writes the results to a CSV file and a rewritten synonyms text file.

    Args:
        shingles_dict (dict): Dictionary containing shingle information.
    """
    with open(SYNONYMS_TXT, mode='r', encoding='utf-8') as synonyms_file, \
         open(SYNONYM_MATCHES_CSV, mode='w', newline='', encoding='utf-8') as synonym_matches_file, \
         open(REWRITTEN_SYNONYMS_TXT, mode='w', encoding='utf-8') as rewritten_synonyms_file:
        
        synonym_writer = csv.writer(synonym_matches_file)
        # Write the header to the SynonymMatches CSV
        synonym_writer.writerow(["Left Term", "Match Type", "Entity", "Original Line", "Rewritten Line"])

        # Process each line from the synonyms file
        for line in synonyms_file:
            process_synonym_line(line.strip(), synonym_writer, rewritten_synonyms_file, shingles_dict)

def process_synonym_line(line: str, csv_writer: csv.writer, txt_writer, shingles_dict: dict) -> None:
    """
    Processes a single synonym line and writes the appropriate data to the CSV and text files.

    Args:
        line (str): The line to process from the synonyms file.
        csv_writer (csv.writer): CSV writer object to write to the SynonymMatches CSV.
        txt_writer: File writer object to write to the rewritten synonyms text file.
        shingles_dict (dict): Dictionary containing shingle information.
    """
    if '=>' in line:
        left_term, right_terms = [term.strip() for term in line.split('=>', 1)]
        right_terms_list = [term.strip() for term in right_terms.split(',')]
        
        match_type, entity_type = get_match_info(left_term, right_terms_list, shingles_dict)
        
        original_line = line
        rewritten_line = f"{left_term} => {left_term}, {', '.join(right_terms_list)}"
        
        # Write the processed synonym to the CSV
        csv_writer.writerow([left_term, match_type, entity_type, original_line, rewritten_line])
        
        # Write the rewritten line to the text file
        txt_writer.write(rewritten_line + '\n')
        
        print(f"Processed synonym: {left_term}")
    else:
        # Write lines without '=>' unchanged to the text file
        txt_writer.write(line + '\n')

def get_match_info(left_term: str, right_terms: list, shingles_dict: dict) -> tuple:
    """
    Retrieves match type and entity type for a given left term from the shingles dictionary.

    Args:
        left_term (str): The left term from the synonym line.
        right_terms (list): List of right terms in the synonym line.
        shingles_dict (dict): Dictionary containing shingle information.

    Returns:
        tuple: A tuple containing match type and entity type, or empty strings if no match is found.
    """
    lower_left_term = left_term.lower()
    if lower_left_term in shingles_dict and left_term not in right_terms:
        matching_entries = shingles_dict[lower_left_term]
        for entry in matching_entries:
            return entry[1], entry[2]  # match_type, entity_type
    
    return "", ""  # Default to empty strings if no match found

process_synonyms(shingles_dict)