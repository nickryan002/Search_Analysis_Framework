import os
import pandas as pd

def append_csv_files(directory, output_file):
    # Initialize an empty list to hold dataframes
    dataframes = []

    # Loop through all files in the given directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            # Read the CSV file into a dataframe
            df = pd.read_csv(filepath)
            # Append the dataframe to the list
            dataframes.append(df)
            print(f"Processed file: {filename}")

    # Concatenate all dataframes in the list
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Write the combined dataframe to a new CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"All files have been appended and saved to {output_file}")

if __name__ == "__main__":
    # Specify the directory containing the CSV files and the output file name
    input_directory = "ClientData/Catalog Feeds US and CA Aug 21/US"
    output_csv = "ClientData/full_catalog.csv"

    # Call the function to append CSV files
    append_csv_files(input_directory, output_csv)