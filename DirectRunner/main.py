import json
import logging
import os

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv

load_dotenv()

project_id = os.environ.get("PROJECT_ID")
job_name = os.environ.get("JOB_NAME", "velo-lib-amiens-dataflow")

dataset_id = os.environ.get("DATASET_NAME", "velo_lib_dataset")
table_id = os.environ.get("TABLE_ID", "amiens")
gcloud_app_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "./../paas-gcp-insset-2023-301962e7807a.json")

# The ID of your GCS bucket
bucket_name = os.environ.get("BUCKET_NAME")
temp_bucket_name = os.environ.get("TEMP_BUCKET_NAME")

# Default, timeout = 5 seconds
timeout = int(os.environ.get("TIMEOUT", "10"))

empty_data = []

beam_options = PipelineOptions(
    runner="DirectRunner",
    project=project_id,
    job_name=job_name
)

table_config = bigquery.TableReference(
    projectId=project_id,
    datasetId=dataset_id,
    tableId=table_id
)

table_schema = {
    "fields": [
        {"name": "station_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "station_number", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "station_address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "station_latitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "station_longitude", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "station_bike_stands", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "station_available_bike_stands", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "station_available_bikes", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "station_status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "station_last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}


class TreatStation(beam.DoFn):
    def process(self, stations):
        return [station for station in stations]


def run():
    with beam.Pipeline(options=beam_options) as p:
        (p | "Read" >> beam.io.ReadFromText("gs://velo-lib-amiens/velo-data_2023-10-16_17-15-01.json")
         | "Read data" >> beam.Map(lambda line: json.loads(line))
         | "Treat bucket data (from single data to station list)" >> beam.ParDo(TreatStation())
         | "Retrieve station data" >> beam.Map(
                    lambda station: {
                        # station['name'] if station['name'] is not None else ""
                        "station_name": station.get("name", ""),
                        "station_number": station.get("number", None),
                        "station_address": station.get("address", None),
                        "station_latitude": station["position"]["lat"] if station["position"]["lat"] is not None else None,
                        "station_longitude": station["position"]["lng"] if station["position"]["lng"] is not None else None,
                        "station_bike_stands": station.get("bike_stands", None),
                        "station_available_bike_stands": station.get("available_bike_stands", None),
                        "station_available_bikes": station.get("available_bikes", None),
                        "station_status": station.get("status", None),
                        "station_last_update": int(round(station.get("last_update") / 1000)) if station.get("last_update") is not None else None,
                    }
                )
         # | "Data treatment" >> beam.Map(lambda name: {'station_name': name})
         | "Save data" >> beam.io.WriteToBigQuery(table_config, schema=table_schema,
                                                  custom_gcs_temp_location=temp_bucket_name,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
         )

        # import pandas as pd
        #
        # pd.to_datetime(int('1331856000000'), utc=True, unit='ms')

        p.run()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
