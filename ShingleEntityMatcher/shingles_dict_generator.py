import csv
import requests
from sortedcontainers import SortedDict
import normalizer
import copy

# Global SortedDict to store shingles with their corresponding details
shingles_dict = SortedDict()

# Read from a CSV file and populate the shingles dictionary
def read_csv_and_populate_shingles_dict(filename, shingles_dict):
    """
    Reads data from a CSV file and populates the shingles dictionary.

    Args:
        filename (str): The path to the CSV file.
        shingles_dict (SortedDict): The dictionary to populate with shingles.
    """
    print(f"Opening file {filename} to populate shingles dictionary...")
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        print("Reading headers...")
        for row in reader:
            for i, entity in enumerate(row):
                if entity.strip():  # Ensure the entity is not empty
                    entity_type = headers[i]
                    add_shingles_to_dict(entity, entity_type, shingles_dict)
    print("Shingles dictionary populated successfully.")
    expand_shingles_with_normalization(shingles_dict)

# Generate shingles for an entity and add them to the shingles dictionary
def add_shingles_to_dict(entity, entity_type, shingles_dict):
    """
    Generates shingles for a given entity and adds them to the shingles dictionary.

    Args:
        entity (str): The entity to generate shingles for.
        entity_type (str): The type of the entity (e.g., category, collection).
        shingles_dict (SortedDict): The dictionary to populate with shingles.
    """
    entity_shingles = generate_shingles(entity)
    for shingle in entity_shingles:
        shingle_key = shingle.lower()
        shingle_type = "full" if shingle.lower() == entity.lower() else "partial"
        if shingle_key not in shingles_dict:
            shingles_dict[shingle_key] = []
        shingles_dict[shingle_key].append([entity, shingle_type, entity_type, ""])

# Generate shingles from a phrase
def generate_shingles(phrase):
    """
    Generates all possible shingles from a phrase.

    Args:
        phrase (str): The phrase to generate shingles from.

    Returns:
        list: A list of shingles generated from the phrase.
    """
    words = phrase.split()
    shingles = [' '.join(words[i:j+1]) for i in range(len(words)) for j in range(i, len(words))]
    return shingles

# Function to expand the shingles dictionary using normalized keys
def expand_shingles_with_normalization(shingles_dict):
    """
    Expands the shingles dictionary by adding normalized versions of shingles.

    Args:
        shingles_dict (SortedDict): The dictionary containing shingles to expand with normalization.
    """
    print("Expanding shingles dictionary with normalized keys...")
    original_keys = list(shingles_dict.keys())
    for original_key in original_keys:
        if len(original_key.split()) == 1:  # Only normalize single-word shingles
            normalized_result = normalizer.normalize(original_key, 'dig_practice_char_stem')
            normalized_key = normalized_result["result"][0]

            if normalized_key and normalized_key != original_key:
                filter_changes = append_true_keys(normalized_result)

                shingles_dict_original_key_with_normalization = copy.deepcopy(shingles_dict[original_key])
                for sublist in shingles_dict_original_key_with_normalization:
                    sublist[3] = filter_changes

                if normalized_key in shingles_dict:
                    shingles_dict[normalized_key].extend(shingles_dict_original_key_with_normalization)
                else:
                    shingles_dict[normalized_key] = shingles_dict_original_key_with_normalization

    print("Shingles dictionary expanded successfully.")

# Function to append all keys with True values into a string separated by slashes
def append_true_keys(filter_changes):
    """
    Appends all keys with True values into a single string separated by slashes.

    Args:
        filter_changes (dict): A dictionary containing filter change information.

    Returns:
        str: A string of keys with True values, separated by slashes.
    """
    true_keys = [key for key, value in filter_changes.items() if value == True]
    return '/'.join(true_keys)
