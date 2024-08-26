import csv
from collections import defaultdict
from decimal import Decimal
import sys
import normalizer  # Assuming you have a normalizer module with a normalize function

def normalize_and_aggregate(input_filename: str, output_filename: str) -> None:
    """
    Normalizes search queries from an input CSV file, aggregates their visits and revenue,
    and writes the results to an output CSV file.

    Args:
        input_filename (str): Path to the input CSV file containing search queries.
        output_filename (str): Path to the output CSV file where normalized and aggregated results will be saved.
    """
    print("Normalizing and aggregating search queries...")

    # Dictionary to hold aggregated data with the format {normalized_query: [total_visits, total_revenue]}
    aggregated_data = defaultdict(lambda: [0, Decimal('0.00')])
    iteration_count = 0

    with open(input_filename, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        clean_header(reader)

        for row in reader:
            iteration_count += 1
            if iteration_count % 1000 == 0:
                sys.stdout.write(f"\rQueries processed: {iteration_count}")
                sys.stdout.flush()

            search_query = row['Search Query']
            normalized_search_query = normalizer.get_normalized_final_text(search_query, 'dig_practice_char')
            visits = int(row['Visits'])
            revenue = Decimal(row['Revenue'].replace('$', '').replace(',', ''))

            # Aggregate visits and revenue based on the normalized search query
            aggregated_data[normalized_search_query][0] += visits
            aggregated_data[normalized_search_query][1] += revenue

    write_aggregated_data(input_filename, output_filename, aggregated_data)

    print("Search queries successfully normalized and aggregated.")

def clean_header(reader: csv.DictReader) -> None:
    """
    Cleans the CSV header to remove any BOM from the first column name.

    Args:
        reader (csv.DictReader): The CSV reader object.
    """
    if reader.fieldnames and reader.fieldnames[0].startswith('\ufeff'):
        reader.fieldnames[0] = reader.fieldnames[0].replace('\ufeff', '')

def write_aggregated_data(input_filename: str, output_filename: str, aggregated_data: dict) -> None:
    """
    Writes the aggregated search query data to an output CSV file.

    Args:
        input_filename (str): Path to the input CSV file.
        output_filename (str): Path to the output CSV file.
        aggregated_data (dict): Dictionary containing aggregated data.
    """
    with open(output_filename, mode='w', newline='', encoding='utf-8') as outfile:
        print("\nWriting normalized and aggregated queries to new CSV...")
        with open(input_filename, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            clean_header(reader)

            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                normalized_search_query = normalizer.get_normalized_final_text(row['Search Query'], 'dig_practice_char')

                if normalized_search_query in aggregated_data:
                    visits, revenue = aggregated_data[normalized_search_query]
                    row['Search Query'] = normalized_search_query
                    row['Visits'] = visits
                    row['Revenue'] = revenue
                    writer.writerow(row)

                    # Remove the entry after writing it to avoid duplicates
                    del aggregated_data[normalized_search_query]
