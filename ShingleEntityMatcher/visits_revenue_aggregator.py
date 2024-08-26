import csv
from collections import defaultdict
from decimal import Decimal
import sys
import normalizer  # Assuming you have a normalizer module with a normalize function

def normalize_and_aggregate(input_filename, output_filename):
    print("Normalizing and aggregating search queries...")

    # Dictionary to hold aggregated data
    aggregated_data = defaultdict(lambda: [0, Decimal('0.00')])
    iteration_count = 0
    
    with open(input_filename, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)

        # Remove BOM from the first column name if present
        if reader.fieldnames and reader.fieldnames[0].startswith('\ufeff'):
            reader.fieldnames[0] = reader.fieldnames[0].replace('\ufeff', '')

        headers = reader.fieldnames

        for row in reader:
            iteration_count += 1
            # Update and print the iteration count every 1000 iterations
            if iteration_count % 1000 == 0:
                sys.stdout.write(f"\rQueries processed: {iteration_count}")
                sys.stdout.flush()
            
            search_query = row['Search Query']
            normalized_search_query = normalizer.get_normalized_final_text(search_query, 'dig_practice_char')
            #normalized_search_query = normalized_result['result']
            visits = int(row['Visits'])
            revenue = Decimal(row['Revenue'].replace('$', '').replace(',', ''))
            
            # Aggregate visits and revenue based on the normalized search query
            aggregated_data[normalized_search_query][0] += visits
            aggregated_data[normalized_search_query][1] += revenue

    # Write the aggregated data to a new CSV file
    with open(output_filename, mode='w', newline='', encoding='utf-8') as outfile:
        print("Writing normalized and aggregated queries to new csv...")
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()  # Write the header row

        with open(input_filename, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)

            # Remove BOM from the first column name if present
            if reader.fieldnames and reader.fieldnames[0].startswith('\ufeff'):
                reader.fieldnames[0] = reader.fieldnames[0].replace('\ufeff', '')

            for row in reader:
                search_query = row['Search Query']
                normalized_search_query = normalizer.get_normalized_final_text(search_query, 'dig_practice_char')
                #normalized_search_query = normalized_result['result']
                if normalized_search_query in aggregated_data:
                    visits, revenue = aggregated_data[normalized_search_query]
                    row['Search Query'] = normalized_search_query
                    row['Visits'] = visits
                    row['Revenue'] = revenue
                    writer.writerow(row)
                    # Remove the entry after writing it to avoid duplicates
                    del aggregated_data[normalized_search_query]

    print("Search queries successfully normalized and aggregated.")