#Install the Azure Data Lake Storage client library
#pip install azure-storage-file-datalake
#Import libraries
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from datetime import datetime


def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
        return service_client
    
    except Exception as e:
        print(e)



def create_directory():
    try:
        file_system_client = service_client.get_file_system_client(file_system="my-container/"
        file_system_client.create_directory(datetime.now().strftime('%Y%m%d'))
    
    except Exception as e:
     print(e)

def get_files():
        try:
            file_paths = []
            file_path = './'+datetime.now().strftime('%Y%m%d')

            for folder, subs, files in os.walk(file_path):
                for filename in files:
                    file_paths.append(os.path.abspath(os.path.join(folder, filename)))
            return file_paths
        except Exception as e:
            print(e)


def upload_file_to_directory():
        try:

            file_system_client = service_client.get_file_system_client(file_system="my-container/")

            directory_client = file_system_client.get_directory_client(datetime.now().strftime('%Y%m%d'))

            for files in get_files():
                    
                file_client = directory_client.create_file(files.split('\\')[-1])
                local_file = open(files,'rb')

                file_contents = local_file.read()

                file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

                file_client.flush_data(len(file_contents))
            

        except Exception as e:
            print(e)


if __name__ == '__main__':
    storage_account_name = 'storage_name'
    # storage_key will look something like this 'bE2J8nK6RlfpUVyD3i8zGlkF0bJBpORYv5n9f0og6972CY7rQ3tPWf+ASthnmR7A=='
    storage_account_key = 'storage_key'
    initialize_storage_account(storage_account_name, storage_account_key)
    create_directory()
    upload_file_to_directory()
    