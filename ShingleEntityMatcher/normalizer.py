import requests

# Function to normalize text by analyzing it using Solr's analysis endpoint
def normalize(text_to_analyze, desired_field_type):
    solr_url = "http://localhost:8983/solr"
    core_name = "catalog_core"
    field_type = desired_field_type
    analysis_result = analyze_text_HTTP(solr_url, core_name, field_type, text_to_analyze)

    return get_normalized_result(analysis_result, field_type, text_to_analyze)

def get_raw_normalized_result(text_to_analyze, desired_field_type):
    solr_url = "http://localhost:8983/solr"
    core_name = "catalog_core"
    field_type = desired_field_type
    analysis_result = analyze_text_HTTP(solr_url, core_name, field_type, text_to_analyze)

    return analysis_result.get('analysis', {}).get('field_types', {}).get(field_type, {}).get('index', [])

def get_normalized_final_text(text_to_analyze, desired_field_type):
    solr_url = "http://localhost:8983/solr"
    core_name = "catalog_core"
    field_type = desired_field_type
    analysis_result = analyze_text_HTTP(solr_url, core_name, field_type, text_to_analyze)

    normalized_result = analysis_result.get('analysis', {}).get('field_types', {}).get(field_type, {}).get('index', [])

    # Get the last filter in the JSON data
    last_filter = normalized_result[-1]
    
    # Extract the 'text' fields from the last filter
    texts = [token['text'] for token in list(last_filter.values())[0]]
    
    # Join the texts with a space separator
    return ' '.join(texts)

# Function to send a request to Solr's analysis endpoint to analyze the text
def analyze_text_HTTP(solr_url, core_name, field_type, text_to_analyze):
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

# Function to extract the normalized text from Solr's analysis response
def get_normalized_result(response, field_type, original_text):
    analysis_result = response.get('analysis', {}).get('field_types', {}).get(field_type, {}).get('index', [])
    if not analysis_result:
        return {}
    
    print(analysis_result)

    filter_changes = {}
    previous_texts = [original_text]

    for phase in analysis_result:
        if isinstance(phase, dict):  # Check if phase is a dictionary
            for filter_name, tokens in phase.items():
                filter_name = filter_name.split('.')[-1]
                current_texts = []
                if isinstance(tokens, list):
                    current_texts = [token['text'] for token in tokens if isinstance(token, dict) and 'text' in token]
                elif isinstance(tokens, str):
                    current_texts = [tokens]
                if current_texts:
                    #print(f"Current texts: {current_texts} and previous text: {previous_texts}")
                    filter_changes[filter_name] = current_texts != previous_texts
                    # filter_changes[filter_name] = any(token != previous_text for token in current_texts)
                    previous_texts = current_texts  # Update previous_text for the next phase

    filter_changes["result"] = current_texts
    #print(append_true_keys(filter_changes))

    return filter_changes

# Function to append all keys with True values into a string separated by slashes
def append_true_keys(filter_changes):
    true_keys = [key for key, value in filter_changes.items() if value == True]
    return '/'.join(true_keys)

if __name__ == "__main__":
    text_to_analyze = "shorts dresses 25\""
    # response = normalize(text_to_analyze, 'dig_practice_char_syns_stem')
    # print(f"Final result: {response}")

    # response = get_raw_normalized_result(text_to_analyze, 'dig_practice_char_syns_stem')
    # print(f"Raw result: {response}")

    response = get_normalized_final_text(text_to_analyze, 'dig_practice_char_stem')
    print(f"Final text result: {response}")
