import pysolr
import re

# Connect to the Solr server
solr_url = 'http://localhost:8983/solr/catalog_core'
solr = pysolr.Solr(solr_url, always_commit=True)

def escape_solr_query(value: str) -> str:
    """
    Escapes special characters in a Solr query string to avoid syntax errors.

    Args:
        value (str): The value to escape.

    Returns:
        str: The escaped value with special characters escaped.
    """
    # Use regex to escape special Solr characters
    return re.sub(r'([+\-&|!(){}[\]^"~*?:\\])', r'\\\1', value)

def check_normalized_values_in_row(values: list, shingles_dict: dict) -> bool:
    """
    Checks if a set of normalized values exist in the same row in Solr.

    Args:
        values (list): List of values to check.
        shingles_dict (dict): Dictionary containing shingles information.

    Returns:
        bool: True if there are any rows in Solr that match the query, otherwise False.
    """
    query_parts = []

    # Build Solr query parts based on shingles_dict entries
    for value in values:
        if value in shingles_dict:
            value_queries = []
            for item in shingles_dict[value]:
                val, _, entity_type, _ = item
                # Append _t to entity_type to match Solr field names
                entity_type_t = f"{entity_type}_t"
                # Escape the value to prevent Solr syntax errors
                escaped_val = escape_solr_query(val)
                value_queries.append(f'{entity_type_t}:"{escaped_val}"')
            if value_queries:
                # Combine multiple value queries using OR
                query_parts.append(f'({" OR ".join(value_queries)})')

    # Combine all query parts into a single Solr query using AND
    combined_query = " AND ".join(query_parts)

    # Perform the Solr query
    results = solr.search(combined_query, rows=200000)

    # Check if any rows matched the query
    rows_found = len(results)
    return rows_found > 0

def check_unnormalized_values_in_row(values: list, shingles_dict: dict) -> bool:
    """
    Checks if a set of unnormalized values exist in the same row in Solr.

    Args:
        values (list): List of values to check.
        shingles_dict (dict): Dictionary containing shingles information.

    Returns:
        bool: True if there are any rows in Solr that match the query, otherwise False.
    """
    query_parts = []

    # Build Solr query parts based on shingles_dict entries
    for value in values:
        if value in shingles_dict:
            value_queries = []
            for item in shingles_dict[value]:
                val, _, entity_type, filter = item
                if filter == "":
                    # Append _t to entity_type to match Solr field names
                    entity_type_t = f"{entity_type}_t"
                    # Escape the value to prevent Solr syntax errors
                    escaped_val = escape_solr_query(val)
                    value_queries.append(f'{entity_type_t}:"{escaped_val}"')
            if value_queries:
                # Combine multiple value queries using OR
                query_parts.append(f'({" OR ".join(value_queries)})')

    # Combine all query parts into a single Solr query using AND
    combined_query = " AND ".join(query_parts)
    # Optionally, print the combined query for debugging purposes
    # print(f"Combined Solr Query: {combined_query}")

    # Perform the Solr query
    results = solr.search(combined_query, rows=200000)

    # Check if any rows matched the query
    rows_found = len(results)
    return rows_found > 0
