import io
import os
import csv
import zipfile
import requests
import apache_beam as beam

class ParseCSVRow(beam.DoFn):
    """Parse a CSV line into a list."""
    def process(self, line, has_header=False):
        reader = csv.reader([line])
        for row in reader:
            yield row

class ExpandPhones(beam.DoFn):
    """
    Expands each row into multiple rows if multiple phone columns are present.
    """
    def __init__(self, phone_columns):
        self.phone_columns = phone_columns

    def process(self, row_list):
        expanded_rows = []
        for col_idx in self.phone_columns:
            if col_idx < len(row_list):
                new_row = list(row_list)
                primary_phone = new_row[col_idx]
                transformed_row = [primary_phone] + new_row
                expanded_rows.append(transformed_row)
        return expanded_rows

class BatchRowsToSingleFile(beam.DoFn):
    """
    Batches rows into a single CSV file payload.
    """
    def process(self, rows_list):
        output = io.StringIO()
        writer = csv.writer(output)
        for row in rows_list:
            writer.writerow(row)
        csv_data = output.getvalue()
        yield csv_data

class CallBlacklistAPI(beam.DoFn):
    """
    Sends CSV data to the Blacklist Alliance API and extracts the resulting ZIP file.
    
    Assumptions:
      - The CSV data is provided as a string.
      - The API expects a multipart/form-data POST where:
          - The file is attached using the key "file" with content type "text/csv".
          - Other required parameters are passed as form fields.
      - The API returns a ZIP file that contains three CSV files (invalid, clean, dnc).
    """
    def process(self, csv_data):
        url = "https://api.blacklistalliance.net/bulk/upload"
        
        file_tuple = (
            "expanded.csv",
            io.BytesIO(csv_data.encode("utf-8")),
            "text/csv"
        )
        
        files = {
            "file": file_tuple
        }
        
        payload = {
            "filetype": "csv",
            "download_carrier_data": "false",
            "download_invalid": "true",
            "download_no_carrier": "false",
            "download_wireless": "false",
            "download_federal_dnc": "true",
            "splitchar": ",",
            "key": os.getenv("BLACKLIST_API_KEY"),
            "colnum": "1"
        }
        
        headers = {
            "accept": "application/zip"
        }
        
        response = requests.post(url, data=payload, files=files, headers=headers)
        if response.status_code != 200:
            raise RuntimeError(f"Blacklist API call failed: {response.status_code} - {response.text}")
        
        zip_bytes = response.content
        results = {}
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zfile:
            for name in zfile.namelist():
                file_content = zfile.read(name).decode("utf-8")
                lower_name = name.lower()
                if "all_clean" in lower_name:
                    results["all_clean"] = file_content
                elif "invalid" in lower_name:
                    results["invalid"] = file_content
                elif "federal_dnc" in lower_name:
                    results["federal_dnc"] = file_content
                else:
                    results[name] = file_content
        
        yield results

class MergeLeadsAndRemoveDuplicates(beam.DoFn):
    """
    Deduplicates CSV content from the API and merges leads with different phone numbers.
    """
    def process(self, file_content_dict):
        output = {}
        for file_name, csv_str in file_content_dict.items():
            rows = csv.reader(csv_str.splitlines())
            header = next(rows)  # Assuming the first row is the header
            leads_dict = {}
            for row in rows:
                lead_key = tuple(row[1:])  # Use all columns except the phone number as the key
                phone_number = row[0]
                if lead_key in leads_dict:
                    leads_dict[lead_key].add(phone_number)
                else:
                    leads_dict[lead_key] = {phone_number}
            
            merged_rows = [header]
            for lead_key, phone_numbers in leads_dict.items():
                merged_row = [",".join(phone_numbers)] + list(lead_key)
                merged_rows.append(merged_row)
            
            output_csv = io.StringIO()
            writer = csv.writer(output_csv)
            writer.writerows(merged_rows)
            output[file_name] = output_csv.getvalue()
        
        yield output
