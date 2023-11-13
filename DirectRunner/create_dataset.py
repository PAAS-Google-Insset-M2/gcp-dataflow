import os

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# Default, timeout = 5 seconds
timeout = int(os.environ.get("TIMEOUT", "10"))

client = bigquery.Client()

# Configure dataset id
dataset_name = os.environ.get("DATASET_NAME", "velo_lib_dataset")
dataset_id = f"{client.project}.{dataset_name}"

# Construct dataset
dataset = bigquery.Dataset(dataset_id)

# Put dataset in the same location as the bucket
dataset_location = os.environ.get("DATASET_LOCATION", "europe-west9")
dataset.location = dataset_location

# Create dataset
dataset = client.create_dataset(dataset, timeout=timeout)
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
# Created dataset paas-gcp-insset-2023.velo_lib_dataset
