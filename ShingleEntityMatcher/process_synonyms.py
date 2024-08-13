import csv

SYNONYMS_TXT = 'lulu_solr_synonyms.txt'
SYNONYM_MATCHES_CSV = 'SynonymExpansions.csv'
REWRITTEN_SYNONYMS_TXT = 'RewrittenSynonyms.txt'  # New file for rewritten synonyms

# Process synonyms and write to SynonymMatches CSV and RewrittenSynonyms TXT.
def process_synonyms(shingles_dict): 
    with open(SYNONYMS_TXT, mode='r', encoding='utf-8') as synonyms_file, \
         open(SYNONYM_MATCHES_CSV, mode='w', newline='', encoding='utf-8') as synonym_matches_file, \
         open(REWRITTEN_SYNONYMS_TXT, mode='w', encoding='utf-8') as rewritten_synonyms_file:  # New file writer
        
        synonym_writer = csv.writer(synonym_matches_file)
        synonym_writer.writerow(["Left Term", "Match Type", "Entity", "Original Line", "Rewritten Line"])

        for line in synonyms_file:
            process_synonym_line(line, synonym_writer, rewritten_synonyms_file, shingles_dict)

# Process a single synonym line and write to the SynonymMatches CSV and RewrittenSynonyms TXT.
def process_synonym_line(line, csv_writer, txt_writer, shingles_dict):
    line = line.strip()
    if '=>' in line:
        left_term, right_terms = line.split('=>', 1)
        left_term = left_term.strip()
        right_terms = [term.strip() for term in right_terms.split(',')]
        
        match_type, entity_type = get_match_info(left_term, right_terms, shingles_dict)
        
        original_line = line
        rewritten_line = f"{left_term} => {left_term}, {', '.join(right_terms)}"
        csv_writer.writerow([left_term, match_type, entity_type, original_line, rewritten_line])
        txt_writer.write(rewritten_line + '\n')  # Write to the text file
        print(f"Processed synonym: {left_term}")
    else:
        # Write lines without '=>' unchanged to the text file
        txt_writer.write(line + '\n')
        # print(f"Unchanged line: {line}")

# Get match type and entity type for a left term.
def get_match_info(left_term, right_terms, shingles_dict):
    lower_left_term = left_term.lower()
    if lower_left_term in shingles_dict and left_term not in right_terms:
        matching_entries = shingles_dict[lower_left_term]
        for entry in matching_entries:
            return entry[1], entry[2]  # match_type, entity_type
    return "", ""  # Default to blank if no match found
