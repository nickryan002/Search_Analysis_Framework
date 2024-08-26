import csv
import normalizer
import synonym_string_list_generator

def rollup_queries(input_csv_path: str) -> None:
    """
    Normalizes and expands problematic search queries from the input CSV, 
    then rolls up similar queries and writes the results to an output CSV.

    Args:
        input_csv_path (str): Path to the input CSV file.
    """
    normalized_queries = []
    normalized_queries_expanded = []

    with open(input_csv_path, newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header

        print("Normalizing and expanding problematic search queries...")

        for row in csvreader:
            if row:
                original_query = row[0]
                # Normalize the original query
                normalized_query = normalizer.get_normalized_final_text(original_query, 'dig_practice_char_stem')
                normalized_queries.append(normalized_query)

                # Normalize and expand the query with synonyms
                normalized_query_expanded_result = normalizer.get_raw_normalized_result(original_query, 'dig_practice_char_syns_stem')
                normalized_query_expanded = synonym_string_list_generator.reconstruct_strings(normalized_query_expanded_result)
                normalized_queries_expanded.append('/'.join(normalized_query_expanded))

        print("Normalization completed.")
        print("Rolling up similar queries...")
        process_csv(input_csv_path, "rolled_up_queries.csv", normalized_queries, normalized_queries_expanded)
        print("Roll up completed.")

def normalize_revenue(revenue_str: str) -> float:
    """
    Normalizes the revenue string by removing any dollar signs and commas.

    Args:
        revenue_str (str): The revenue string to normalize.

    Returns:
        float: The normalized revenue as a float.
    """
    return float(revenue_str.replace('$', '').replace(',', ''))

def process_csv(input_csv: str, output_csv: str, normalized_queries: list, expanded_queries: list) -> None:
    """
    Processes the input CSV to aggregate and roll up similar queries, then writes the results to an output CSV.

    Args:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to the output CSV file.
        normalized_queries (list): List of normalized queries.
        expanded_queries (list): List of expanded queries.
    """
    rows = []
    aggregation_dict = {}
    rolled_up_indices = set()  # Set to track rolled-up indices
    
    with open(input_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    # Iterate through the normalized queries in List A
    for a_index, normalized_query in enumerate(normalized_queries):
        if a_index in rolled_up_indices:
            continue  # Skip if this query has already been rolled up

        visits_a = int(rows[a_index]['Visits'])
        revenue_a = normalize_revenue(rows[a_index]['Revenue'])
        normalization_filters_a = set(rows[a_index]['Normalization Filters'].split('/'))

        # Initialize the aggregation dictionary for this query
        aggregation_dict[normalized_query] = [a_index, visits_a, revenue_a, [], normalization_filters_a]

        # Iterate through the expanded queries in List B
        for b_index, query_b in enumerate(expanded_queries):
            if b_index == a_index:
                continue  # Skip if the index is the same

            queries_b_split = query_b.split('/')
            if normalized_query in queries_b_split:
                matched_row = rows[b_index]
                visits_b = int(matched_row['Visits'])
                revenue_b = normalize_revenue(matched_row['Revenue'])
                normalization_filters_b = set(matched_row['Normalization Filters'].split('/'))     

                # Update the aggregation dictionary
                if visits_b > aggregation_dict[normalized_query][1]:
                    aggregation_dict[normalized_query][0] = b_index  # Update best index if current visits are higher

                aggregation_dict[normalized_query][1] += visits_b  # Aggregate visits
                aggregation_dict[normalized_query][2] += revenue_b  # Aggregate revenue
                aggregation_dict[normalized_query][3].append(matched_row["Problematic Search Query"])  # Add the matched query
                aggregation_dict[normalized_query][4].update(normalization_filters_b)  # Add normalization filters

                rolled_up_indices.add(b_index)  # Track this index as rolled up

    # Write the rolled-up queries to the output CSV
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = list(rows[0].keys()) + ['Rolled Up Queries']  # Add a column for rolled-up queries
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for subquery, (best_index, total_visits, total_revenue, rolled_up_queries, all_filters) in aggregation_dict.items():
            best_row = rows[best_index].copy()
            best_row['Visits'] = str(total_visits)
            best_row['Revenue'] = total_revenue

            # Combine all normalization filters and update the best row
            combined_filters = '/'.join(sorted(filter(None, all_filters)))
            best_row['Normalization Filters'] = combined_filters

            # Update the best row with the rolled-up queries
            best_row['Rolled Up Queries'] = '/'.join(rolled_up_queries)
            writer.writerow(best_row)

# Example usage
if __name__ == "__main__":
    input_csv_path = 'ShingleEntityMatcher/problematic_searches_new.csv'  # Replace with the path to your CSV file
    rollup_queries(input_csv_path)
