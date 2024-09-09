import csv
import normalizer
import synonym_string_list_generator

def rollup_queries(input_csv_path, output_csv_path):
    normalized_queries = []
    normalized_queries_expanded = []

    with open(input_csv_path, newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        # Skip the header
        next(csvreader)

        print ("Rolling up similar queries...")

        for row in csvreader:
            if row:
                original_query = row[0]
                normalized_query = normalizer.get_normalized_final_text(original_query, 'dig_practice_char_stem')
                normalized_queries.append(normalized_query)

                normalized_query_expanded_result = normalizer.get_raw_normalized_result(original_query, 'dig_practice_char_syns_stem')
                normalized_query_expanded = synonym_string_list_generator.reconstruct_strings(normalized_query_expanded_result)
                normalized_queries_expanded.append('/'.join(normalized_query_expanded))

        process_csv(input_csv_path, output_csv_path, normalized_queries, normalized_queries_expanded)
        print ("Roll up completed.")

# Function to normalize revenue (removing $ and commas)
def normalize_revenue(revenue_str):
    return float(revenue_str.replace('$', '').replace(',', ''))

# Function to format revenue back to string
def format_revenue(revenue_float):
    return f"${revenue_float:,.2f}"

# Function to process the input CSV and generate the output CSV
def process_csv(input_csv, output_csv, list_a, list_b):
    rows = []
    aggregation_dict = {}
    rolled_up_indices = set()  # Set to keep track of rolled-up indices
    
    with open(input_csv, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            rows.append(row)

    # Iterate through List A
    for a_index, normalized_query in enumerate(list_a):
        if a_index in rolled_up_indices:
            continue  # Skip processing if this query has already been rolled up

        visits_a = int(rows[a_index]['Visits'])
        revenue_a = normalize_revenue(rows[a_index]['Revenue'])
        normalization_filters_a = rows[a_index]['Normalization Filters'].split('/')

        aggregation_dict[normalized_query] = [a_index, visits_a, revenue_a, [], set(normalization_filters_a)]

        for b_index, query_b in enumerate(list_b):
            queries_b_split = query_b.split('/')
            if normalized_query in queries_b_split and b_index != a_index:
                matched_row = rows[b_index]
                visits_b = int(matched_row['Visits'])
                revenue_b = normalize_revenue(matched_row['Revenue'])
                normalization_filters = matched_row['Normalization Filters'].split('/')     

                if normalized_query in aggregation_dict:
                    if visits_b > aggregation_dict[normalized_query][1]:
                        aggregation_dict[normalized_query][0] = b_index  # Update best index if current visits are higher
                        
                    aggregation_dict[normalized_query][1] += visits_b  # Aggregate visits
                    aggregation_dict[normalized_query][2] += revenue_b  # Aggregate revenue
                    aggregation_dict[normalized_query][3].append(rows[b_index]["Problematic Search Query"])  # Add the matched query to the list
                    aggregation_dict[normalized_query][4].update(normalization_filters)  # Add normalization filters

                # Add the b_index to the rolled_up_indices set to ignore it later
                rolled_up_indices.add(b_index)

    # Write the final CSV
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        fieldnames = rows[0].keys()
        fieldnames = list(fieldnames) + ['Rolled Up Queries']  # Add new column for rolled-up queries
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for subquery, (best_index, total_visits, total_revenue, rolled_up_queries, all_filters) in aggregation_dict.items():
            best_row = rows[best_index].copy()
            best_row['Visits'] = str(total_visits)
            best_row['Revenue'] = format_revenue(total_revenue)

            # Combine all normalization filters and update the best row's normalization filters
            combined_filters = '/'.join(sorted(filter(None, all_filters)))
            best_row['Normalization Filters'] = combined_filters

            # Update the best row with the rolled-up queries
            best_row['Rolled Up Queries'] = '/'.join(rolled_up_queries)
            writer.writerow(best_row)
