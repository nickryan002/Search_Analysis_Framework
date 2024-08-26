import requests

SOLR_URL = "http://localhost:8983/solr"
CORE_NAME = "catalog_core"

def normalize(text_to_analyze: str, desired_field_type: str) -> dict:
    """
    Normalizes text by analyzing it using Solr's analysis endpoint.

    Args:
        text_to_analyze (str): The text to be analyzed and normalized.
        desired_field_type (str): The Solr field type to use for analysis.

    Returns:
        dict: A dictionary containing the normalized text and filter changes.
    """
    analysis_result = analyze_text(solr_url=SOLR_URL, core_name=CORE_NAME, field_type=desired_field_type, text_to_analyze=text_to_analyze)
    return get_normalized_result(analysis_result, desired_field_type, text_to_analyze)

def get_raw_normalized_result(text_to_analyze: str, desired_field_type: str) -> list:
    """
    Retrieves the raw normalized result from Solr's analysis response.

    Args:
        text_to_analyze (str): The text to be analyzed.
        desired_field_type (str): The Solr field type to use for analysis.

    Returns:
        list: A list of tokenized results from Solr's analysis.
    """
    analysis_result = analyze_text(solr_url=SOLR_URL, core_name=CORE_NAME, field_type=desired_field_type, text_to_analyze=text_to_analyze)
    return analysis_result.get('analysis', {}).get('field_types', {}).get(desired_field_type, {}).get('index', [])

def get_normalized_final_text(text_to_analyze: str, desired_field_type: str) -> str:
    """
    Extracts and returns the final normalized text from Solr's analysis response.

    Args:
        text_to_analyze (str): The text to be analyzed.
        desired_field_type (str): The Solr field type to use for analysis.

    Returns:
        str: The final normalized text, concatenated as a single string.
    """
    normalized_result = get_raw_normalized_result(text_to_analyze, desired_field_type)

    # Extract the 'text' fields from the last filter phase
    last_filter = normalized_result[-1] if normalized_result else {}
    texts = [token['text'] for token in list(last_filter.values())[0]] if last_filter else []
    
    return ' '.join(texts)

def analyze_text(solr_url: str, core_name: str, field_type: str, text_to_analyze: str) -> dict:
    """
    Sends a request to Solr's analysis endpoint to analyze the text.

    Args:
        solr_url (str): The base URL of the Solr instance.
        core_name (str): The name of the Solr core to query.
        field_type (str): The Solr field type to use for analysis.
        text_to_analyze (str): The text to analyze.

    Returns:
        dict: The JSON response from Solr containing the analysis result.
    """
    analysis_url = f"{solr_url}/{core_name}/analysis/field"
    params = {
        'wt': 'json',
        'json.nl': 'arrmap',
        'analysis.fieldtype': field_type,
        'analysis.fieldvalue': text_to_analyze
    }
    response = requests.get(analysis_url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def get_normalized_result(response: dict, field_type: str, original_text: str) -> dict:
    """
    Extracts the normalized text and tracks filter changes from Solr's analysis response.

    Args:
        response (dict): The JSON response from Solr's analysis.
        field_type (str): The Solr field type used for analysis.
        original_text (str): The original text that was analyzed.

    Returns:
        dict: A dictionary with filter changes and the final normalized result.
    """
    analysis_result = response.get('analysis', {}).get('field_types', {}).get(field_type, {}).get('index', [])
    if not analysis_result:
        return {}

    filter_changes = {}
    previous_texts = [original_text]

    # Iterate over each phase in the analysis result to track changes in text
    for phase in analysis_result:
        if isinstance(phase, dict):
            for filter_name, tokens in phase.items():
                filter_name = filter_name.split('.')[-1]  # Extract the filter name
                current_texts = extract_texts_from_tokens(tokens)
                
                if current_texts:
                    filter_changes[filter_name] = current_texts != previous_texts
                    previous_texts = current_texts  # Update previous_texts for the next phase

    filter_changes["result"] = previous_texts  # Store the final normalized result

    return filter_changes

def extract_texts_from_tokens(tokens) -> list:
    """
    Extracts text from a list of tokens.

    Args:
        tokens: A list of tokens or a single string.

    Returns:
        list: A list of extracted texts.
    """
    if isinstance(tokens, list):
        return [token['text'] for token in tokens if isinstance(token, dict) and 'text' in token]
    elif isinstance(tokens, str):
        return [tokens]
    return []

def append_true_keys(filter_changes: dict) -> str:
    """
    Appends all keys with True values into a string separated by slashes.

    Args:
        filter_changes (dict): A dictionary of filter changes with boolean values.

    Returns:
        str: A string of keys with True values, separated by slashes.
    """
    true_keys = [key for key, value in filter_changes.items() if value is True]
    return '/'.join(true_keys)
