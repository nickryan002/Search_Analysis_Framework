import pandas as pd
import re

def clean_data(input_csv: str, output_csv: str) -> None:
    """
    Cleans the data from the input CSV file and writes the cleaned data to an output CSV file.

    Args:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to the output CSV file where cleaned data will be saved.
    """
    # Read the input CSV into a DataFrame
    df = pd.read_csv(input_csv)

    # Get the total number of rows to process
    total_rows = len(df)
    print(f"Total rows to process: {total_rows}")

    def clean_and_lowercase(text: str) -> str:
        """
        Cleans and lowercases the given text.

        Args:
            text (str): The text to clean and lowercase.
            replace_colon (bool): Whether to replace '::::' with '/' in the text.

        Returns:
            str: The cleaned and lowercased text.
        """
        if pd.isna(text):
            return ""
        text = str(text).lower()
        text = text.replace('::::', ' / ')
        text = text.replace('-', ' ')
        text = text.replace('*', '')
        text = text.replace('|', '')
        return text

    def remove_urls_and_lowercase(text: str) -> str:
        """
        Removes URLs from the text and lowercases it.

        Args:
            text (str): The text from which to remove URLs.

        Returns:
            str: The text with URLs removed and lowercased.
        """
        if pd.isna(text):
            return ""
        # Use regex to remove URLs
        text = re.sub(r'\|https?:\/\/.*?(?=\||$)', '', text)
        return text.lower()

    def process_row(row: pd.Series, index: int) -> pd.Series:
        """
        Processes a single row of the DataFrame by cleaning and normalizing its fields.

        Args:
            row (pd.Series): The row to process.
            index (int): The index of the row in the DataFrame.

        Returns:
            pd.Series: The processed row.
        """
        # Print progress for every 1000 rows processed
        if index % 1000 == 0:
            print(f"Processed {index} rows out of {total_rows}")
        
        # Clean and normalize specific fields in the row
        row['parentCategory_displayName'] = clean_and_lowercase(row.get('parentCategory_displayName', '')).replace(' & ', '/')
        row['sku_colorGroup'] = remove_urls_and_lowercase(row.get('sku_colorGroup', ''))
        row['sku_colorGroup_fr'] = remove_urls_and_lowercase(row.get('sku_colorGroup_fr', ''))
        row['product_activity'] = clean_and_lowercase(row.get('product_activity', ''))
        row['product_customAttribute4'] = clean_and_lowercase(row.get('product_customAttribute4', ''))
        row['gender'] = clean_and_lowercase(row.get('gender', ''))
        row['sku_size'] = clean_and_lowercase(row.get('sku_size', ''))
        row['product_topsLength_s'] = clean_and_lowercase(row.get('product_topsLength_s', ''))
        row['collections'] = clean_and_lowercase(row.get('collections', ''))
        row['sku_colorCodeDesc'] = clean_and_lowercase(row.get('sku_colorCodeDesc', ''))
        row['product_inseam'] = clean_and_lowercase(row.get('product_inseam', ''))
        row['product_displayName'] = clean_and_lowercase(row.get('product_displayName', ''))
        row['product_feel'] = clean_and_lowercase(row.get('product_feel', ''))
        row['product_fit'] = clean_and_lowercase(row.get('product_fit', ''))
        row['product_function'] = clean_and_lowercase(row.get('product_function', ''))
        row['sku_sizeType_ss'] = clean_and_lowercase(row.get('sku_sizeType_ss', ''))
        row['product_rise_s'] = clean_and_lowercase(row.get('product_rise_s', ''))
        row['product_title'] = clean_and_lowercase(row.get('product_title', ''))
        row['product_gender'] = clean_and_lowercase(row.get('product_gender', ''))

        return row

    # Apply the processing function to each row in the DataFrame
    df = df.apply(lambda row: process_row(row, df.index.get_loc(row.name)), axis=1)

    # Select the columns to keep in the final cleaned DataFrame
    columns_to_keep = [
        'gender', 'parentCategory_displayName', 'sku_size', 'product_topsLength_s',
        'collections', 'sku_colorCodeDesc', 'sku_colorGroup', 'sku_colorGroup_fr',
        'product_inseam', 'product_activity', 'product_customAttribute4', 'product_displayName', 
        'product_feel', 'product_fit', 'product_function', 'sku_sizeType_ss', 'product_rise_s', 
        'product_title', 'product_gender'
    ]
    cleaned_df = df[columns_to_keep]

    # Write the cleaned data to the output CSV file
    cleaned_df.to_csv(output_csv, index=False, na_rep='')

    print(f"Processed {total_rows} rows successfully.")

# Example usage
if __name__ == "__main__":
    input_csv = 'CatalogNormalizer/full_catalog.csv'
    output_csv = 'CatalogNormalizer/simplified_catalog.csv'
    clean_data(input_csv, output_csv)
