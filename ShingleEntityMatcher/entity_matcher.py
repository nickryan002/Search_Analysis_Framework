import csv
from sortedcontainers import SortedDict
import visits_revenue_aggregator
import catalog_match_checker
import shingles_dict_generator
import process_synonyms

# Global variables for real filenames - Uncomment/comment to use
ENTITY_TABLE_CSV = 'ShingleEntityMatcher/entity_table.csv'
MATCHED_TABLE_CSV = 'ShingleEntityMatcher/MatchedTable.csv'
UNMATCHED_TABLE_CSV = 'ShingleEntityMatcher/UnmatchedTable.csv'
LULU_TERMS_CSV = 'ShingleEntityMatcher/lulu_terms.csv'
LULU_TERMS_AGGREGATED_CSV = 'ShingleEntityMatcher/lulu_terms_Aggregated.csv'
SYNONYMS_TXT = 'ShingleEntityMatcher/lulu_solr_synonyms.txt'
SYNONYM_MATCHES_CSV = 'ShingleEntityMatcher/SynonymExpansions.csv'
PROBLEMATIC_SEARCHES_CSV = 'ShingleEntityMatcher/problematic_searches_new.csv'

# # Global variables for test filenames - Uncomment/comment to use
# ENTITY_TABLE_CSV = 'ShingleEntityMatcher/entity_table.csv'
# MATCHED_TABLE_CSV = 'ShingleEntityMatcher/125_MatchedTable.csv'
# UNMATCHED_TABLE_CSV = 'ShingleEntityMatcher/125_UnmatchedTable.csv'
# LULU_TERMS_CSV = 'ShingleEntityMatcher/searchTerms-125.csv'
# LULU_TERMS_AGGREGATED_CSV = 'ShingleEntityMatcher/searchTerms-125_Aggregated.csv'
# SYNONYMS_TXT = 'ShingleEntityMatcher/lulu_solr_synonyms.txt'
# SYNONYM_MATCHES_CSV = 'ShingleEntityMatcher/SynonymExpansions.csv'
# PROBLEMATIC_SEARCHES_CSV = 'ShingleEntityMatcher/problematic_searches_new.csv'

# Global SortedDict to store shingles with their corresponding details
shingles_dict = SortedDict()

# Function to write the sorted dictionary to a text file
def write_dict_to_file(dictionary, file_name):
    with open(file_name, 'w') as file:
        for key, value in dictionary.items():
            file.write(f'{key}: {value}\n')

# Write matched and unmatched shingles to their respective CSV files.
def write_to_matched_unmatched_csvs(search_query, shingles, visits, revenue): 
    #print(f"Writing to CSVs for the search query: {search_query}")
    with open(MATCHED_TABLE_CSV, mode='a', newline='', encoding='utf-8') as matched_file, \
         open(UNMATCHED_TABLE_CSV, mode='a', newline='', encoding='utf-8') as unmatched_file:
        
        matched_writer = csv.writer(matched_file)
        unmatched_writer = csv.writer(unmatched_file)
        
        for shingle in shingles:
            lower_shingle = shingle.lower()
            if lower_shingle in shingles_dict:
                write_matched_shingles(matched_writer, shingle, search_query, visits, revenue)
            else:
                unmatched_writer.writerow([shingle, search_query, visits, revenue])
                #print(f"Unmatched: {shingle}")

# Write matched shingles to the matched CSV file
def write_matched_shingles(writer, shingle, search_query, visits, revenue):
    lower_shingle = shingle.lower()
    matched_entities = {}
    partial_matches = []

    for entry in shingles_dict[lower_shingle]:
        entity = entry[0]
        entity_type = entry[1]
        if entity_type not in matched_entities:
            matched_entities[entity_type] = []
        matched_entities[entity_type].append(entity)
        if entry[1] == "partial":
            partial_matches.append(entity)

    # Determine if there are overlaps
    entity_overlap = len(set([item for sublist in matched_entities.values() for item in sublist])) > 1
    entity_type_overlap = len(matched_entities) > 1

    if entity_type_overlap:
        entities_str = '|'.join([f"{'|'.join(matched_entities[key])}" for key in matched_entities])
        entity_types_str = '|'.join(matched_entities.keys())
    else:
        entities_str = '|'.join(matched_entities[next(iter(matched_entities))])
        entity_types_str = next(iter(matched_entities.keys()))

    partial_matches_str = '|'.join(partial_matches) if partial_matches else shingle

    writer.writerow([
        shingle, partial_matches_str, "partial" if partial_matches else "full", 
        entity_types_str, search_query, visits, revenue,
        "yes" if entity_overlap or entity_type_overlap else "no",
        len(set([item for sublist in matched_entities.values() for item in sublist])),
        len(matched_entities)
    ])
    #print(f"Matched: {shingle}")


# Initialize MatchedTable, UnmatchedTable, and ProblematicSearches CSVs with headers.
def initialize_csvs():
    with open(MATCHED_TABLE_CSV, mode='w', newline='', encoding='utf-8') as matched_file, \
         open(UNMATCHED_TABLE_CSV, mode='w', newline='', encoding='utf-8') as unmatched_file, \
         open(PROBLEMATIC_SEARCHES_CSV, mode='w', newline='', encoding='utf-8') as problematic_file:
        matched_writer = csv.writer(matched_file)
        unmatched_writer = csv.writer(unmatched_file)
        problematic_writer = csv.writer(problematic_file)
        matched_writer.writerow([
            "Matched Shingle", "Entities", "Shingle Type", "Entity Type", "Search Query", "Visits", "Revenue",
            "Overlap", "Entity Overlaps", "Entity Type Overlaps"
        ])
        unmatched_writer.writerow(["Unmatched Shingle", "Search Query", "Visits", "Revenue"])
        problematic_writer.writerow(["Problematic Search Query", "Legitimate", "Catalog Field", "Normalization Filters", "Visits", "Revenue"])

# Process search queries from aggregated terms CSV.
def process_search_queries():
    with open(LULU_TERMS_AGGREGATED_CSV, mode='r', newline='', encoding='utf-8') as search_queries_file, \
         open(PROBLEMATIC_SEARCHES_CSV, mode='a', newline='', encoding='utf-8') as problematic_file:
        reader = csv.reader(search_queries_file)
        problematic_writer = csv.writer(problematic_file)
        next(reader)  # Skip the header
        print("Processing search queries...")
        for row in reader:
            search_phrase = row[0]
            visits = row[4]
            revenue = row[8]
            shingles = shingles_dict_generator.generate_shingles(search_phrase)
            write_to_matched_unmatched_csvs(search_phrase, shingles, visits, revenue)

            # Check each token in the shingles dictionary and catalog checker
            tokens = search_phrase.split()
            tokens_in_dict = [token for token in tokens if token.lower() in shingles_dict]

            # entity_types = [shingles_dict[token.lower()][0][2] for token in tokens_in_dict]
            # entity_types_str = "/".join(entity_types)

            entity_types = extract_dict_info(tokens, "entity_type")

            #FIRST PASS - NO NORMALIZATION APPLIED

            if len(tokens) > 1 and len(entity_types.split("/")) > 1 and tokens == tokens_in_dict:        
                if not catalog_match_checker.check_unnormalized_values_in_row(tokens_in_dict, shingles_dict):
                    problematic_query_row = [search_phrase, "N", entity_types, "", visits, revenue]

                    #SECOND PASS - NORMALIZATION APPLIED
                    if catalog_match_checker.check_values_in_row(tokens_in_dict, shingles_dict):
                        problematic_query_row = [search_phrase, "Y", entity_types, extract_dict_info(tokens_in_dict, "filter"), visits, revenue]
                    else:
                        problematic_query_row = [search_phrase, "N", entity_types, extract_dict_info(tokens_in_dict, "filter"), visits, revenue]

                    problematic_writer.writerow(problematic_query_row)
                    print(f"Problematic search query: {search_phrase}")

        print("Finished processing all search queries.")

def extract_dict_info(tokens, type):
    # Initialize a set to collect unique filters across all tokens
    all_dict_info_set = set()

    # Iterate through each token in the list
    for token in tokens:
        # Check if the token exists in the dictionary
        if token in shingles_dict:
            # Get the list of lists associated with the token
            lists = shingles_dict[token]
            # Iterate through each sublist
            for sublist in lists:
                # Check if the last element is not an empty string
                if type == "entity_type":
                    if sublist[2]:
                        # Add the third element to the set
                        all_dict_info_set.add(sublist[2])
                if type == "filter":
                    if sublist[-1]:
                        # Add the last element to the set
                        all_dict_info_set.add(sublist[-1])

    # Join the unique filters into a single string
    final_result = "/".join(all_dict_info_set)

    return final_result

def main():
    shingles_dict_generator.read_csv_and_populate_shingles_dict(ENTITY_TABLE_CSV, shingles_dict)
    write_dict_to_file(shingles_dict, 'ShingleEntityMatcher/dictionary.txt')
    # visits_revenue_aggregator.normalize_and_aggregate(LULU_TERMS_CSV, LULU_TERMS_AGGREGATED_CSV)
    initialize_csvs()
    process_search_queries()
    #process_synonyms.process_synonyms(shingles_dict)

if __name__ == "__main__":
    main()
