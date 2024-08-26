import pysolr
import csv

# Initialize Solr client with increased timeout
SOLR_URL = 'http://localhost:8983/solr/catalog_core'
solr = pysolr.Solr(SOLR_URL, always_commit=True, timeout=60)

def delete_all_documents() -> None:
    """
    Deletes all documents in the Solr collection.
    """
    solr.delete(q='*:*')
    solr.commit()
    print("All documents deleted successfully.")

def convert_to_dynamic_fields(document: dict) -> dict:
    """
    Converts field names to dynamic field types by appending '_t' to each field name.

    Args:
        document (dict): The document with original field names.

    Returns:
        dict: A document with dynamic field names.
    """
    return {f"{key}_t": value for key, value in document.items()}

def read_and_ingest_to_solr(filename: str, batch_size: int = 100) -> None:
    """
    Reads search queries from a CSV file and ingests them into Solr in batches.

    Args:
        filename (str): Path to the CSV file containing the data.
        batch_size (int): Number of documents to batch together before sending to Solr.
    """
    delete_all_documents()  # Ensure Solr is cleared before ingesting new data
    print("Reading data from catalog and ingesting into Solr...")

    documents = []
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read the header row to get field names

        for row in reader:
            # Create a document by mapping header names to row values
            document = {headers[i]: row[i] for i in range(len(headers))}
            dynamic_document = convert_to_dynamic_fields(document)  # Convert to dynamic fields
            documents.append(dynamic_document)

            # Ingest the documents in batches
            if len(documents) >= batch_size:
                solr.add(documents)
                documents.clear()  # Clear the list after batch is added

    # Ingest any remaining documents that didn't fill the last batch
    if documents:
        solr.add(documents)

    print("Data ingested into Solr successfully.")

if __name__ == "__main__":
    read_and_ingest_to_solr('CatalogNormalizer/simplified_catalog.csv')
