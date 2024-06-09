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



#save contentCsvConsolidation content 
def save_contentCsvConsolidation(content,caseid,filename):
    try:
        logging.info(f"save_ContentByClinicAreas start, content: {content},caseid: {caseid},filename: {filename}")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/ContentByClinicAreas/contentCsvConsolidation/{filename}"
        # Upload the blob and overwrite if it already exists
        blob_client = container_client.upload_blob(name=destinationPath, data=content, overwrite=True)
        logging.info(f"the ContentByClinicAreas content file url is: {blob_client.url}")
        return destinationPath
    
    except Exception as e:
        print("An error occurred:", str(e))

#save contentCsvNoDuplicates content 
def save_contentCsvNoDuplicates(content,caseid,filename):
    try:
        logging.info(f"save_ContentByClinicAreas start, content: {content},caseid: {caseid},filename: {filename}")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/ContentByClinicAreas/contentCsvNoDuplicates/{filename}"
        # Upload the blob and overwrite if it already exists
        blob_client = container_client.upload_blob(name=destinationPath, data=content, overwrite=True)
        logging.info(f"the ContentByClinicAreas content file url is: {blob_client.url}")
        return destinationPath
    
    except Exception as e:
        print("An error occurred:", str(e))

 #Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        print("Event created successfully.")
    
    except Exception as e:
        print("An error occurred:", str(e))

def merge_csv_rows_by_diagnosis(csv_string):
    # Read the input CSV string
    input_csv = StringIO(csv_string)
    reader = csv.DictReader(input_csv)

    # Dictionary to store merged rows by diagnosis
    merged_data = defaultdict(lambda: {
        'diagnosis': '',
        'dateofdiagnosis': '',
        'levelstageseverity': '',
        'treatment': '',
        'page_number': ''
    })

    # Process each row and merge data
    for row in reader:
        diagnosis = row['diagnosis']
        merged_row = merged_data[diagnosis]

        # Merge the fields
        if not merged_row['diagnosis']:
            merged_row['diagnosis'] = diagnosis

        for field in ['dateofdiagnosis', 'levelstageseverity', 'treatment', 'page_number']:
            current_value = merged_row[field]
            new_value = row[field]
            if current_value:
                #merged_row[field] = f"{current_value},{field} {len(current_value.splitlines()) + 1} - {new_value}"
                merged_row[field] = f"{current_value},{new_value}"
            else:
                #merged_row[field] = f"{field} 1 - {new_value}"
                merged_row[field] = f"{new_value}"

    # Prepare output CSV
    output_csv = StringIO()
    fieldnames = ['diagnosis', 'dateofdiagnosis', 'levelstageseverity', 'treatment','page_number']
    writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
    writer.writeheader()
    for merged_row in merged_data.values():
        writer.writerow(merged_row)

    # Return the merged CSV string
    return output_csv.getvalue()

# Update field on specific entity/ row in storage table 
def update_entity_field(table_name, partition_key, row_key, field_name, new_value):

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

# get content csv from azure storage 
def get_contentcsv_from_storage(path):
    try:
        logging.info(f"get_contentcsv function strating, path value: {path}")
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        download_stream = blob_client.download_blob()
        filecontent  = download_stream.read().decode('utf-8')
        logging.info(f"get_contentcsv: data from the txt file is {filecontent}")
        #encoded_content_csv = entity.get('contentCsv')
        retrieved_csv = filecontent.replace('\\n', '\n') 
        return retrieved_csv
    except Exception as e:
        logging.error(f"get_contentcsv: Error update case: {str(e)}")
        return None    


def get_content_Csv_path(table_name, partition_key, row_key):

    try:
        # Create a TableServiceClient using the connection string
        service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient for the specified table
        table_client = service_client.get_table_client(table_name=table_name)

        # Retrieve the entity using PartitionKey and RowKey
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

        # Return the value of 'contentAnalysisCsv' field
        content_path= entity.get('contentCsv')
        #retrieved_csv = encoded_content_csv.replace('\\n', '\n') 
        return content_path
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
    content_csv_path = get_content_Csv_path(sourceTable, caseid, clinicArea)
    logging.info(f"csv content path: {content_csv_path}")
    content_csv = get_contentcsv_from_storage(content_csv_path)
    unique_content_csv = remove_duplicates(content_csv)
    logging.info(f"csv content: {unique_content_csv}")
    encoded_content_csv = unique_content_csv.replace('\n', '\\n')
    #update csv after exact duplicate removal
    filename = f"{clinicArea}.txt"
    contentCsvNoDuplicates_path = save_contentCsvNoDuplicates(encoded_content_csv,caseid,filename)
    update_entity_field(sourceTable, caseid, clinicArea, "contentCsvNoDuplicates", contentCsvNoDuplicates_path)
    #mege csv content by diagnosis
    merged_csv = merge_csv_rows_by_diagnosis(unique_content_csv)
    encoded_merged_csv = merged_csv.replace('\n', '\\n')
    contentCsvConsolidation_path = save_contentCsvConsolidation(encoded_merged_csv,caseid,filename)
    update_entity_field(sourceTable, caseid, clinicArea, "contentCsvConsolidation", contentCsvConsolidation_path)
    #preparing data for service bus
    data = { 
                "clinicArea" : clinicArea, 
                "storageTable" :sourceTable,
                "caseid" :caseid
            } 
    json_data = json.dumps(data)
    create_servicebus_event("niimatchingrules",json_data)