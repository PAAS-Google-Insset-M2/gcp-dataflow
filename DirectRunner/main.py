import os
import json
import apache_beam as beam

from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

from dotenv import load_dotenv

load_dotenv()

beam_options = PipelineOptions (
    runner="",
    project="",
    job_name=""
)

# gcloud_app_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./paas-gcp-insset-2023-301962e7807a.json"

api_key = os.environ.get("JCDECAUX_API_KEY")

contract_name = os.environ.get("JCDECAUX_CONTRACT", "amiens")

# The ID of your GCS bucket
bucket_name = os.environ.get("BUCKET_NAME")

# Default, timeout = 5 seconds
timeout = int(os.environ.get("TIMEOUT", "5"))

empty_data = []
