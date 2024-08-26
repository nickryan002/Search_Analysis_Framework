import pandas as pd
import re

def clean_data(input_csv, output_csv):
    # Read the input CSV
    df = pd.read_csv(input_csv)

    total_rows = len(df)
    print(f"Total rows to process: {total_rows}")

    # Function to clean and lowercase text
    def clean_and_lowercase(text, replace_colon=False):
        if pd.isna(text):
            return ""
        text = str(text).lower()
        if replace_colon:
            text = text.replace('::::', '/')
        return text

    # Function to remove URLs and lowercase text
    def remove_urls_and_lowercase(text):
        if pd.isna(text):
            return ""
        text = re.sub(r'\|https?:\/\/.*?(?=\||$)', '', text)
        return text.lower()

    # Function to process each row and print progress
    def process_row(row, index):
        if index % 1000 == 0:
            print(f"Processed {index} rows out of {total_rows}")
        row['parentCategory_displayName'] = clean_and_lowercase(row.get('parentCategory_displayName', '')).replace(' & ', '/')
        row['sku_colorGroup'] = remove_urls_and_lowercase(row.get('sku_colorGroup', ''))
        row['sku_colorGroup_fr'] = remove_urls_and_lowercase(row.get('sku_colorGroup_fr', ''))
        row['product_activity'] = clean_and_lowercase(row.get('product_activity', ''), replace_colon=True)
        row['product_customAttribute4'] = clean_and_lowercase(row.get('product_customAttribute4', ''), replace_colon=True)
        row['gender'] = clean_and_lowercase(row.get('gender', ''))
        row['sku_size'] = clean_and_lowercase(row.get('sku_size', ''))
        row['product_topsLength_s'] = clean_and_lowercase(row.get('product_topsLength_s', ''))
        row['collections'] = clean_and_lowercase(row.get('collections', ''))
        row['sku_colorCodeDesc'] = clean_and_lowercase(row.get('sku_colorCodeDesc', ''))
        row['product_inseam'] = clean_and_lowercase(row.get('product_inseam', ''))
        return row

    # Apply the processing function to each row
    df = df.apply(lambda row: process_row(row, df.index.get_loc(row.name)), axis=1)

    # Select the columns to write to the new CSV
    columns_to_keep = [
        'gender', 'parentCategory_displayName', 'sku_size', 'product_topsLength_s',
        'collections', 'sku_colorCodeDesc', 'sku_colorGroup', 'sku_colorGroup_fr',
        'product_inseam', 'product_activity', 'product_customAttribute4'
    ]
    cleaned_df = df[columns_to_keep]

    # Write the cleaned data to the output CSV
    cleaned_df.to_csv(output_csv, index=False, na_rep='')

    print(f"Processed {total_rows} rows successfully.")

# Example usage
input_csv = 'CatalogNormalizer/full_catalog.csv'
output_csv = 'CatalogNormalizer/simplified_catalog.csv'
clean_data(input_csv, output_csv)
