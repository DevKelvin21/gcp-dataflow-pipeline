import argparse
import json
import os
import csv
import io
import logging
import zipfile

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import storage, firestore

# Import utility and transforms from your package.
from dataflow_pipeline.utils import fetch_config_from_firestore
from dataflow_pipeline.transforms import (
    ParseCSVRow,
    ExpandPhones,
    BatchRowsToSingleFile,
    CallBlacklistAPI,
    MergeLeadsAndRemoveDuplicates
)

# -----------------------------------------------------------------------------
# Custom composite DoFns to glue together your transforms.
# -----------------------------------------------------------------------------

class ReadAndFetchConfig(beam.DoFn):
    """
    Reads the file from GCS (using parameters from the Pub/Sub message) and fetches
    the processing configuration from Firestore.
    
    Expects the Pub/Sub message to contain:
      - fileId
      - fileName
      - bucket
      - configDocumentPath (e.g. 'scrubFilesConfig/<docID>')
    """
    def process(self, message):
        # Fetch config from Firestore.
        config = fetch_config_from_firestore(message["configDocumentPath"])
        # Read the file from GCS.
        storage_client = storage.Client()
        bucket = storage_client.bucket(message["bucket"])
        blob = bucket.blob(message["fileName"])
        file_content = blob.download_as_text()
        yield (message, config, file_content)

class PrepareCSV(beam.DoFn):
    """
    Prepares the CSV payload to be sent to the Blacklist API.
    Uses:
      - csv.reader (to parse the file),
      - ExpandPhones (to generate multiple rows per original row if needed), and
      - BatchRowsToSingleFile (to recombine all rows into one CSV string).
      
    Expects a tuple: (message, config, file_content)
    """
    def process(self, element):
        message, config, file_content = element
        # Split into lines and remove header if configured.
        lines = file_content.splitlines()
        if config.get("hasHeaderRow", False) and lines:
            # Optionally store or discard the header.
            lines = lines[1:]
        # Parse CSV rows.
        # (You could also use your ParseCSVRow DoFn per line; here we use csv.reader inline.)
        rows = list(csv.reader(lines))
        # Expand phone columns using your ExpandPhones transform.
        phone_columns = config.get("phoneColumns", [])
        expander = ExpandPhones(phone_columns)
        expanded_rows = []
        for row in rows:
            # The ExpandPhones process() method returns a list of expanded rows.
            expanded_rows.extend(expander.process(row))
        # Batch all expanded rows into a single CSV payload.
        batcher = BatchRowsToSingleFile()
        # Note: batcher.process() returns a generator; we extract the CSV string.
        csv_payload = list(batcher.process(expanded_rows))[0]
        yield (message, config, csv_payload)

class CallAPIAndAttach(beam.DoFn):
    """
    Calls the Blacklist API by re‑using your CallBlacklistAPI transform.
    Attaches the API response (a ZIP file processed into a dict) to the tuple.
    
    Expects a tuple: (message, config, csv_payload)
    """
    def process(self, element):
        message, config, csv_payload = element
        # Instantiate and call your CallBlacklistAPI DoFn.
        api_caller = CallBlacklistAPI()
        for api_response in api_caller.process(csv_payload):
            yield (message, config, csv_payload, api_response)

class AttachDedup(beam.DoFn):
    """
    Deduplicates the CSV content returned by the API.
    Uses your MergeLeadsAndRemoveDuplicates transform.
    
    Expects a tuple: (message, config, csv_payload, api_response)
    """
    def process(self, element):
        message, config, csv_payload, api_response = element
        merger = MergeLeadsAndRemoveDuplicates()
        for dedup_response in merger.process(api_response):
            yield (message, config, csv_payload, dedup_response)

class WriteResults(beam.DoFn):
    """
    Writes each final CSV file to GCS and updates the corresponding Firestore document.
    
    Expects a tuple: (message, config, csv_payload, dedup_response)
    The dedup_response is a dictionary with keys like "all_clean", "invalid", "federal_dnc".
    """
    def __init__(self, output_bucket):
        self.output_bucket = output_bucket

    def setup(self):
        self.storage_client = storage.Client()
        self.db = firestore.Client()

    def process(self, element):
        message, config, csv_payload, results_dict = element
        file_id = message["fileId"]
        output_paths = {}
        bucket = self.storage_client.bucket(self.output_bucket)
        for fn, content in results_dict.items():
            blob_name = f"{file_id}/{fn}"
            blob = bucket.blob(blob_name)
            blob.upload_from_string(content, content_type="text/csv")
            output_paths[fn] = f"gs://{self.output_bucket}/{file_id}/{fn}"
        # Update Firestore with the output file paths and processing status.
        doc_ref = self.db.document(message["configDocumentPath"])
        doc_ref.update({
            "outputFiles": {
                "cleanFilePath": output_paths.get("all_clean", ""),
                "invalidFilePath": output_paths.get("invalid", ""),
                "dncFilePath": output_paths.get("federal_dnc", "")
            },
            "status": {"stage": "DONE"}
        })
        yield f"Processed file_id={file_id}, wrote to {output_paths}"

# -----------------------------------------------------------------------------
# Pipeline definition.
# -----------------------------------------------------------------------------

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', default=os.getenv('GCP_PROJECT'), help='GCP project ID')
    parser.add_argument('--input_topic', required=True,
                        help='Pub/Sub topic to read from (projects/<PROJECT>/topics/<TOPIC>)')
    parser.add_argument('--output_bucket', required=True,
                        help='GCS bucket to write final CSV files to')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args + ['--project', known_args.project], streaming=True, save_main_session=True)
    pipeline_options.view_as(SetupOptions).setup_file = './setup.py'

    p = beam.Pipeline(options=pipeline_options)

    # 1. Read messages from Pub/Sub.
    messages = (
        p
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
        | "DecodeToStr" >> beam.Map(lambda x: x.decode("utf-8"))
        | "ParseJSON" >> beam.Map(json.loads)
    )

    # 2. Read file from GCS and fetch processing configuration from Firestore.
    file_data = messages | "ReadAndFetchConfig" >> beam.ParDo(ReadAndFetchConfig())

    # 3. Prepare the CSV payload ready for the Blacklist API.
    prepared_csv = file_data | "PrepareCSV" >> beam.ParDo(PrepareCSV())

    # 4. Call the Blacklist API and attach its response.
    api_response = prepared_csv | "CallAPIAndAttach" >> beam.ParDo(CallAPIAndAttach())

    # 5. Deduplicate and merge the API response.
    deduped_response = api_response | "AttachDedup" >> beam.ParDo(AttachDedup())

    # 6. Write the results to GCS and update Firestore.
    final_results = deduped_response | "WriteResults" >> beam.ParDo(WriteResults(known_args.output_bucket))

    # (Optional) Log the processing results.
    final_results | "LogResults" >> beam.Map(print)

    p.run().wait_until_finish()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
