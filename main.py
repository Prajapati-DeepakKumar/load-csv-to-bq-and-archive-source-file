import functions_framework
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
import time
from google.cloud import storage
from datetime import datetime


dataset_name = 'Amazon_Sales'
table_name = 'Amazon Sales'
current_date = datetime.now().strftime('%Y%m%d%H%m%s')

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    file_name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    dest_bucket = 'pdks_archive'
    dest_file = f'{current_date}_{file_name}'

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {file_name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    load_csv(file_name)
    archive_file(bucket,file_name,dest_bucket,dest_file)


def load_csv(file_name):
    file_name = file_name
    client = bigquery.Client()
    dest_table = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        autodetect = True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="date",
        ),
    )
    uri = f'gs://pdks_amazon_sales/{file_name}'
    load_job = client.load_table_from_uri(uri,dest_table,job_config=job_config)
    load_job.result()
    print("Loaded {} rows to table {}".format(load_job.output_rows, table_name))


def archive_file(bucket_name,file_name,dest_bucket,dest_file):
    
    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(file_name)
    destination_bucket = storage_client.bucket(dest_bucket)

    destination_generation_match_precondition = 0

    blob_copy = source_bucket.copy_blob(
        source_blob, 
        destination_bucket,
        dest_file, 
        if_generation_match=destination_generation_match_precondition,
    )
    source_bucket.delete_blob(file_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )
