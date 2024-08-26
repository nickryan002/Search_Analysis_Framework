import pandas as pd

def read_config(config_file: str) -> list:
    """
    Reads the configuration file and extracts the list of columns to be used.

    Args:
        config_file (str): Path to the configuration file.

    Returns:
        list: A list of column names specified in the configuration file.
    """
    with open(config_file, 'r') as file:
        columns = file.read().strip().split(',')
    return columns

def create_entity_table(input_csv: str, config_file: str, output_csv: str) -> None:
    """
    Creates an entity table by extracting unique values from specified columns in the input CSV
    and writing the result to an output CSV.

    Args:
        input_csv (str): Path to the input CSV file.
        config_file (str): Path to the configuration file that lists the columns to extract.
        output_csv (str): Path to the output CSV file where the entity table will be saved.
    """
    # Read the input CSV file into a DataFrame
    df = pd.read_csv(input_csv)
    
    # Read the config file to get the column names
    columns = read_config(config_file)
    
    # Initialize an empty DataFrame for the output entity table
    output_df = pd.DataFrame()
    
    # Iterate through the columns specified in the config file
    for column in columns:
        if column in df.columns:
            # Get unique values for the column and reset the index
            unique_values = df[column].drop_duplicates().reset_index(drop=True)
            # Fill the remaining rows with empty strings to match the length of the longest column
            unique_values_filled = unique_values.reindex(range(len(df)), fill_value='')
            # Add the unique values to the output DataFrame under the respective column
            output_df[column] = unique_values_filled
    
    # Remove rows that are entirely empty (all cells are empty)
    output_df = output_df.dropna(how='all')

    # Ensure there are no rows that are completely empty (all cells are empty strings)
    output_df = output_df.loc[~(output_df == '').all(axis=1)]

    # Write the output DataFrame to a new CSV file
    output_df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    input_csv = 'EntityTableGenerator/simplified_catalog.csv'  # Path to the input CSV file
    config_file = 'EntityTableGenerator/config.txt'            # Path to the config file
    output_csv = 'EntityTableGenerator/entity_table_new.csv'   # Path to the output CSV file
    
    create_entity_table(input_csv, config_file, output_csv)
