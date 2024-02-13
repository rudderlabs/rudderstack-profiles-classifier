from google.cloud import bigquery
from google.oauth2 import service_account

def upload_file_to_bigquery(project_id, dataset_id, table_id, file_path, credentials_path):
    # Load credentials from the provided JSON key file
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Initialize a BigQuery client with the loaded credentials
    client = bigquery.Client(project=project_id, credentials=credentials)

    # Get a reference to the dataset
    dataset_ref = client.dataset(dataset_id)

    # Get a reference to the table
    table_ref = dataset_ref.table(table_id)

    print(dataset_ref, table_ref)

    # Load data into the table from a local file
    with open(file_path, "rb") as source_file:
        job_config = bigquery.LoadJobConfig(
            autodetect=True
        )

        job = client.load_table_from_file(
            source_file,
            table_ref,
            location="US",  # Change this to your desired location
            job_config=job_config,
        )  

    # Wait for the job to complete
    job.result()

    print(f"File {file_path} uploaded to table {table_id} in dataset {dataset_id}.")

if __name__ == "__main__":
    # Set your Google Cloud project ID
    project_id = "rudderstacktestbq"

    # Set the dataset ID and table ID where you want to upload the file
    dataset_id = "PROFILES_SIMULATED_DATA"
    table_id = "YOUR_TABLE_NAME"

    # Set the path to the file you want to upload
    file_path = "YOUR_CSV_PATH"

    # Set the path to your service account key file
    credentials_path = "bqkey.json"

    # Call the function to upload the file
    upload_file_to_bigquery(project_id, dataset_id, table_id, file_path, credentials_path)
