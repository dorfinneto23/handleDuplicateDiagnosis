import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
from openai import AzureOpenAI #for using openai services 
from azure.data.tables import TableServiceClient, TableClient, UpdateMode # in order to use azure storage table  
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError # in order to use azure storage table  exceptions 
import csv #helping convert json to csv
from io import StringIO  # in order for merge_csv_rows_by_diagnosis function 
from collections import defaultdict # in order for merge_csv_rows_by_diagnosis function 



# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'



def merge_csv_rows_by_diagnosis(csv_string):
    # Read the input CSV string
    input_csv = StringIO(csv_string)
    reader = csv.DictReader(input_csv)

    # Dictionary to store merged rows by diagnosis
    merged_data = defaultdict(lambda: {
        'diagnosis': '',
        'dateofdiagnosis': '',
        'levelstageseverity': '',
        'treatment': ''
    })

    # Process each row and merge data
    for row in reader:
        diagnosis = row['diagnosis']
        merged_row = merged_data[diagnosis]

        # Merge the fields
        if not merged_row['diagnosis']:
            merged_row['diagnosis'] = diagnosis

        for field in ['dateofdiagnosis', 'levelstageseverity', 'treatment']:
            current_value = merged_row[field]
            new_value = row[field]
            if current_value:
                merged_row[field] = f"{current_value},{field} {len(current_value.splitlines()) + 1} - {new_value}"
            else:
                merged_row[field] = f"{field} 1 - {new_value}"

    # Prepare output CSV
    output_csv = StringIO()
    fieldnames = ['diagnosis', 'dateofdiagnosis', 'levelstageseverity', 'treatment']
    writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
    writer.writeheader()
    for merged_row in merged_data.values():
        writer.writerow(merged_row)

    # Return the merged CSV string
    return output_csv.getvalue()

# Update field on specific entity/ row in storage table 
def update_entity_field(table_name, partition_key, row_key, field_name, new_value):
    """
    Updates a specific field of an entity in an Azure Storage Table.

    Parameters:
    - account_name: str, the name of the Azure Storage account
    - account_key: str, the key for the Azure Storage account
    - table_name: str, the name of the table
    - partition_key: str, the PartitionKey of the entity
    - row_key: str, the RowKey of the entity
    - field_name: str, the name of the field to update
    - new_value: the new value to set for the field
    """
    try:
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient
        table_client = table_service_client.get_table_client(table_name)

        # Retrieve the entity
        entity = table_client.get_entity(partition_key, row_key)

        # Update the field
        entity[field_name] = new_value

        # Update the entity in the table
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        logging.info(f"update_entity_field:Entity updated successfully.")

    except ResourceNotFoundError:
        logging.info(f"The entity with PartitionKey '{partition_key}' and RowKey '{row_key}' was not found.")
    except Exception as e:
        logging.info(f"An error occurred: {e}")

#remove exact duplicates 
def remove_duplicates(csv_string):
    # Read the CSV string into a list of rows
    input_stream = io.StringIO(csv_string)
    reader = csv.reader(input_stream)
    
    # Get the header
    header = next(reader)
    
    # Use a set to track seen rows
    seen = set()
    unique_rows = []

    for row in reader:
        # Convert row to a tuple to make it hashable
        row_tuple = tuple(row)
        if row_tuple not in seen:
            seen.add(row_tuple)
            unique_rows.append(row)

    # Write the unique rows back to a CSV string
    output_stream = io.StringIO()
    writer = csv.writer(output_stream)
    
    # Write the header
    writer.writerow(header)
    
    # Write the unique rows
    writer.writerows(unique_rows)

    # Get the CSV string from the output stream
    output_stream.seek(0)
    return output_stream.getvalue()


def get_content_Csv(table_name, partition_key, row_key):
    """
    Retrieve the 'contentAnalysisCsv' field from the specified Azure Storage Table.

    :param table_name: Name of the table.
    :param partition_key: PartitionKey of the entity.
    :param row_key: RowKey of the entity.
    :param connection_string: Connection string for the Azure Storage account.
    :return: The value of the 'contentAnalysisCsv' field or None if not found.
    """
    try:
        # Create a TableServiceClient using the connection string
        service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient for the specified table
        table_client = service_client.get_table_client(table_name=table_name)

        # Retrieve the entity using PartitionKey and RowKey
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

        # Return the value of 'contentAnalysisCsv' field
        encoded_content_csv = entity.get('contentCsv')
        retrieved_csv = encoded_content_csv.replace('\\n', '\n') 
        return retrieved_csv
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="handleduplicatediagnosis",
                               connection="medicalanalysis_SERVICEBUS") 
def handleDuplicateDiagnosis(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info(f"Received messageesds: {message_data}")
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    clinicArea = message_data_dict['clinicArea']
    sourceTable = message_data_dict['sourceTable']
    logging.info(f"event data:caseid:{caseid},clinicArea:{clinicArea},sourceTable:{sourceTable}")
    content_csv = get_content_Csv(sourceTable, caseid, clinicArea)
    logging.info(f"csv content: {content_csv}")
    unique_content_csv = remove_duplicates(content_csv)
    logging.info(f"csv content: {unique_content_csv}")
    encoded_content_csv = unique_content_csv.replace('\n', '\\n')
    #update csv after exact duplicate removal
    update_entity_field(sourceTable, caseid, clinicArea, "contentCsvNoDuplicates", encoded_content_csv)
    #mege csv content by diagnosis
    merged_csv = merge_csv_rows_by_diagnosis(unique_content_csv)
    encoded_merged_csv = merged_csv.replace('\n', '\\n')
    update_entity_field(sourceTable, caseid, clinicArea, "contentCsvConsolidation", encoded_merged_csv)