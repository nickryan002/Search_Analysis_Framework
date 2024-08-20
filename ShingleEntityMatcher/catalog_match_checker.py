import pysolr
import re

# Connect to the Solr server
solr_url = 'http://localhost:8983/solr/catalog_core'  # Replace with your Solr core URL
solr = pysolr.Solr(solr_url, always_commit=True)

# Function to escape special characters in the query
def escape_solr_query(value):
    return re.sub(r'([+\-&|!(){}[\]^"~*?:\\])', r'\\\1', value)

# Function to check if a set of values are in the same row
def check_values_in_row(values, shingles_dict):

    # Create Solr query parts for each value
    query_parts = []
    for value in values:
        if value in shingles_dict:
            value_queries = []
            for item in shingles_dict[value]:
                val, _, entity_type, filter = item
                # Append _t to entity_type to match the field names in Solr
                entity_type_t = entity_type + '_t'
                # Escape the value
                escaped_val = escape_solr_query(val)
                value_queries.append(f'{entity_type_t}:"{escaped_val}"')
            if value_queries:
                query_parts.append(f'({" OR ".join(value_queries)})')
    # Combine all query parts into a single Solr query
    combined_query = " AND ".join(query_parts)
    #print(f"Combined Solr Query: {combined_query}")

    # Perform the Solr query
    results = solr.search(combined_query, rows=200000)

    # Return True if there are any rows that match the query, otherwise False
    rows_found = len(results)
    return rows_found > 0

# Function to check if a set of values are in the same row
def check_unnormalized_values_in_row(values, shingles_dict):

    # Create Solr query parts for each value
    query_parts = []
    for value in values:
        if value in shingles_dict:
            value_queries = []
            for item in shingles_dict[value]:
                val, _, entity_type, filter = item
                if filter == "":
                    # print(item)
                    # Append _t to entity_type to match the field names in Solr
                    entity_type_t = entity_type + '_t'
                    # Escape the value
                    escaped_val = escape_solr_query(val)
                    value_queries.append(f'{entity_type_t}:"{escaped_val}"')
            if value_queries:
                query_parts.append(f'({" OR ".join(value_queries)})')
    # Combine all query parts into a single Solr query
    combined_query = " AND ".join(query_parts)
    #print(f"Combined Solr Query: {combined_query}")

    # Perform the Solr query
    results = solr.search(combined_query, rows=200000)

    # Return True if there are any rows that match the query, otherwise False
    rows_found = len(results)
    return rows_found > 0
