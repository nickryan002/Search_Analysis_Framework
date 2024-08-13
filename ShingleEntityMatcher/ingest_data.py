import pysolr
import csv

# Initialize Solr client with increased timeout
solr = pysolr.Solr('http://localhost:8983/solr/catalog_core', always_commit=True, timeout=60)

# Function to delete all documents in the Solr collection
def delete_all_documents():
    solr.delete(q='*:*')
    solr.commit()
    print("All documents deleted successfully.")

# Function to convert field names to dynamic field types
def convert_to_dynamic_fields(document):
    return {key + "_t": value for key, value in document.items()}

# Function to read search queries and ingest them into Solr in batches
def read_and_ingest_to_solr(filename, batch_size=100):
    delete_all_documents()
    documents = []
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        for row in reader:
            document = {headers[i]: row[i] for i in range(len(headers))}
            dynamic_document = convert_to_dynamic_fields(document)
            documents.append(dynamic_document)
            if len(documents) >= batch_size:
                solr.add(documents)
                documents = []
                print(f"{batch_size} documents ingested")
    # Ingest any remaining documents
    if documents:
        solr.add(documents)
    print("Data ingested into Solr successfully.")

read_and_ingest_to_solr('CatalogNormalizer/simplified_catalog.csv')
