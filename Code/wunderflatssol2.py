#!/bin/env python
# !/opt/homebrew/bin/python3

import google.cloud.bigquery as bq
import yaml
import argparse
import logging

# Create the parser
parser = argparse.ArgumentParser(description='File path for the config File')

# Add the arguments
parser.add_argument('--path', type=str, required=True)

# Parsing the arguments
args = parser.parse_args()

# assigning the path argument to config_path variable
config_path = args.path  # os.getcwd() + '/config.yaml'
logging.info(f'YAML config file path {config_path}')

# reading the yaml config file for the parameters
with open(config_path, 'r+') as stream:
    yaml_dict = yaml.safe_load(stream=stream)

# assigning the config values to variable
input_file_path = yaml_dict['file_path']
bigquery_table_name = yaml_dict['bigquery_table_name']
file_delimiter = yaml_dict['file_delimiter']
google_auth_file_path = yaml_dict['google_auth_file_path']
logging.info(f'input file path : {input_file_path}')
logging.info(f'bigquery table name : {bigquery_table_name}')
logging.info(f'file delimiter : {file_delimiter}')
logging.info(f' google authentication json file path : {google_auth_file_path}')

# Google Authentication using google_auth file
client = bq.Client.from_service_account_json(google_auth_file_path)
logging.info(f'BigQuery client initiation {client}')

# Loading the configurations for data load
job_config = bq.LoadJobConfig(
    source_format=bq.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
    write_disposition=bq.WriteDisposition.WRITE_APPEND,
    field_delimiter=file_delimiter,
    allow_quoted_newlines=True
)

logging.info(f'Setting up the job config for bigquery jon {job_config}')

# Streaming the CSV data to BigQuery table using load_table_from_file API
with open(file=input_file_path, mode='rb') as stream:
    logging.info(f'loading process is starting')
    job = client.load_table_from_file(stream, bigquery_table_name, job_config=job_config)
    logging.info(f'loading process has been completed')

# it waits till job gets completed
job.result()

table = client.get_table(bigquery_table_name)
logging.info(f'Loaded num of rows {table.num_rows} and columns {table.schema} and table name {bigquery_table_name}')
