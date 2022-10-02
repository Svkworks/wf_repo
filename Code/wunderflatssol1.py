import os
import yaml
import google.cloud.bigquery as bq
import argparse
import logging
import sys

# Create the parser
parser = argparse.ArgumentParser(description='File path for the config File')

# Add the arguments
parser.add_argument('--path', type=str, required=True)

# Parsing the arguments
args = parser.parse_args()

# assigning the path argument to config_path variable
config_path = args.path
logging.info(f'YAML config file path {config_path}')

# reading the yaml config file for the parameters
with open(config_path, 'r+') as stream:
    yaml_dict = yaml.safe_load(stream=stream)

# assigning the config values to variable
input_file_path = yaml_dict['file_path']
file_delimiter = yaml_dict['file_delimiter']
bigquery_table_name = yaml_dict['bigquery_table_name']
google_auth_file_path = yaml_dict['google_auth_file_path']
logging.info(f'input file path : {input_file_path}')
logging.info(f'bigquery table name : {bigquery_table_name}')
logging.info(f'file delimiter : {file_delimiter}')
logging.info(f' google authentication json file path : {google_auth_file_path}')

fh = os.open(input_file_path, flags=os.O_RDONLY)
logging.info(f'File has been opened using os module')

fc = os.read(fh, sys.maxsize)

logging.info('Splitting the data using file delimiter')
data = [l.split(file_delimiter) for l in fc.decode("utf-8").split('\n')]

logging.info('converting nested lists to list of dictionaries')
data = [{data[0][k]: v for k, v in enumerate(l)} for l in data[1:]]

# Google Authentication using google_auth file
client = bq.Client.from_service_account_json(google_auth_file_path)
logging.info(f'BigQuery client initiation {client}')

table_name = client.get_table(bigquery_table_name)
logging.info(f'Loading the data into table : {table_name}')

insert_data = client.insert_rows(table_name, data)

if not insert_data:
    print(f'Success - The data has been loaded into table {table_name}')
else:
    print("Failed - There is an issue with data loading, please look into it")
