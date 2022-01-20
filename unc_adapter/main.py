# Copyright 2022, Battelle Energy Alliance, LLC

import os
import time
import deep_lynx
import json
from dotenv import load_dotenv

def get_job():
    
    containerID, datasourceID, apiClient = deep_lynx_init()

    if containerID is None or datasourceID is None:
        print('Error connecting to Deep Lynx, container ID or datasource ID not acquired')
        return

    while True:
        # query deep lynx for tasks
        taskApi = deep_lynx.TasksApi(apiClient)
        tasks = taskApi.list_tasks(containerID)

        # if a task is found, retrieve it
        for task in tasks.value:

            # query deep lynx using provided import id in task.data
            if not task.data and not "import_id" in task.data:
                print('Task does not contain an import_id')
                continue

            importID = task.data["import_id"]

            importApi = deep_lynx.ImportsApi(apiClient)
            importData = importApi.list_imports_data(containerID, importID)

            # if WRITE_DIR doesn't exist, create it
            if not os.path.exists(os.getenv('WRITE_DIR')):
                os.mkdir(os.getenv('WRITE_DIR'))

            # write file as JSON to WRITE_DIR
            # use the importID to create a unique identifier for this specific run
            write_file(os.path.join(os.getenv('WRITE_DIR'), '{}.json'.format(importID)), importData.value[0].data)

            if os.getenv('UNC_JOB_PATH') is not None and os.getenv('UNC_JOB_PATH') is not '':
                print('call trigger function here')

            # now that the file is written, UNC job will pick it up for processing
            # check for the output files from UNC job and update
            read_flag = False
            while not read_flag:
                # set timer for reading from READ_DIR
                time.sleep(float(os.getenv('READ_FILE_POLL_SECONDS')))

                # retrieve a list of all files in READ_DIR
                unc_out_list = os.listdir(os.getenv('READ_DIR'))

                for unc_out_file in unc_out_list:
                    # for each file found, read in the content
                    file_content = read_file(unc_out_file)

                    if file_content is None:
                        print('File not found: {}'.format(unc_out_file))
                        continue

                    # need to send json/dict, convert if necessary
                    if isinstance(file_content, str):
                        file_content = json.loads(file_content)

                    # if file found, send to deep lynx
                    datasourcesApi = deep_lynx.DataSourcesApi(apiClient)
                    importResult = datasourcesApi.create_manual_import(
                        file_content,
                        containerID,
                        datasourceID
                    )

                    if importResult.is_error is True:
                        print(importResult.error)
                        print('Error sending data to Deep Lynx, continuing')
                        continue

                    # delete read file after successful send
                    delete_file(unc_out_file)
                    print('Successful delete of read file')

                    # mark job as complete
                    taskApi.update_task(containerID, task.id, body=deep_lynx.models.task.Task(
                        status="completed"
                    ))
                    read_flag = True
                    print('Task successfully updated')

        # sleep and then repeat indefinitely
        time.sleep(float(os.getenv('DL_POLL_SECONDS')))


def write_file(file_path, content):
    try:
        # create the file if nessary
        if not os.path.exists(file_path):
            open(file_path, 'x')
        file = open(file_path, 'w')

        # file.write must be given a string
        if isinstance(content, dict):
            content = json.dumps(content)
        
        file.write(content)
        file.close()
    except Exception as e:
        print('Problem writing the file to {}. Please see logs.'.format(file_path))
        print(e)

def read_file(file_path):
    try:
        if os.path.exists(file_path):
            file = open(file_path, 'r')
            content = file.read()
            file.close()
            return content
        return None
    except:
        print('Problem reading the file from {}. Please see logs.'.format(file_path))


def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        return True
    else: 
        print('Error deleting file {}'.format(file_path))
        return False


def deep_lynx_init():
    # authenticate with deep lynx
    # initialize an ApiClient for use with deep_lynx APIs
    configuration = deep_lynx.configuration.Configuration()
    configuration.host = os.getenv('DEEP_LYNX_URL')
    apiClient = deep_lynx.ApiClient(configuration)

    # authenticate via an API key and secret
    authApi = deep_lynx.AuthenticationApi(apiClient)
    token = authApi.retrieve_o_auth_token(x_api_key=os.getenv('DEEP_LYNX_API_KEY'),
        x_api_secret=os.getenv('DEEP_LYNX_API_SECRET'), x_api_expiry='12h')

    # update header
    apiClient.set_default_header('Authorization', 'Bearer {}'.format(token))
    
    # get container ID
    containerID = None
    containerApi = deep_lynx.ContainersApi(apiClient)
    containers = containerApi.list_containers()
    for container in containers.value:
        if container.name == os.getenv('CONTAINER_NAME'):
            containerID = container.id
            continue

    if containerID is None:
        print('Container not found')
        return None, None, None

    # get data source ID, create if necessary
    datasourceID = None
    datasourcesApi = deep_lynx.DataSourcesApi(apiClient)

    datasources = datasourcesApi.list_data_sources(containerID)
    for datasource in datasources.value:
        if datasource.name == os.getenv('DATA_SOURCE_NAME'):
            datasourceID = datasource.id
    if datasourceID is None:
        datasource = datasourcesApi.create_data_source(deep_lynx.models.create_data_source_request.CreateDataSourceRequest(
            os.getenv('DATA_SOURCE_NAME'), 'standard', True
        ), containerID)
        datasourceID = datasource.value.id

    return containerID, datasourceID, apiClient



if __name__ == '__main__':
    # loads environment variables from .env file
    load_dotenv()

    get_job()
