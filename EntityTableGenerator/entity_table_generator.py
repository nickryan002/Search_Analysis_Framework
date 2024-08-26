import pandas as pd

def read_config(config_file):
    with open(config_file, 'r') as file:
        columns = file.read().strip().split(',')
    return columns

def remove_duplicates(input_csv, config_file, output_csv):
    # Read the input CSV file
    df = pd.read_csv(input_csv)
    
    # Read the config file to get the column names
    columns = read_config(config_file)
    
    # Initialize an empty DataFrame for the output
    output_df = pd.DataFrame()
    
    # Iterate through the columns specified in the config file
    for column in columns:
        if column in df.columns:
            # Get the unique values for the column
            unique_values = df[column].drop_duplicates().reset_index(drop=True)
            # Fill the rest of the rows with empty strings to match the length of the longest column
            unique_values_filled = unique_values.reindex(range(len(df)), fill_value='')
            # Add the unique values to the output DataFrame
            output_df[column] = unique_values_filled
    
    # Remove completely empty rows
    output_df = output_df.dropna(how='all')

    # Ensure there are no rows that are entirely empty (all cells are empty)
    output_df = output_df.loc[~(output_df == '').all(axis=1)]

    # Write the output DataFrame to a new CSV file
    output_df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    input_csv = 'EntityTableGenerator/simplified_catalog.csv'       # Path to the input CSV file
    config_file = 'EntityTableGenerator/config.txt'    # Path to the config file
    output_csv = 'EntityTableGenerator/entity_table_new.csv'     # Path to the output CSV file
    
    remove_duplicates(input_csv, config_file, output_csv)
