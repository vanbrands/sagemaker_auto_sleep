import boto3
import requests
from datetime import datetime, timezone

MAX_IDLE_SIZE = 30  # in minutes


def lambda_handler(event, context):

    instance_name = 'XXX'
    sagemaker = boto3.client('sagemaker')
    response = sagemaker.list_notebook_instances(NameContains=instance_name)
    instance_id = response['NotebookInstances'][0]['NotebookInstanceName']

    response = sagemaker.create_presigned_notebook_instance_url(
        NotebookInstanceName=instance_id,
        SessionExpirationDurationInSeconds=600
    )
    url = response['AuthorizedUrl']

    # retrieve the list of running kernels
    response = requests.get(f'{url}/api/kernels')

    for kernel in response.json():

        if kernel['execution_state'] == 'busy':
            continue

        kernel_id = kernel['id']
        response = requests.get(f'{url}/api/kernels/{kernel_id}')
        last_activity_time_str = response.json()['last_activity']
        last_activity_time = datetime.strptime(
            last_activity_time_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)

        # calculate the time elapsed since the last activity time
        current_time = datetime.now(timezone.utc)
        time_elapsed = (
            current_time - last_activity_time).total_seconds() // 60  # in minutes

        if time_elapsed > MAX_IDLE_SIZE:
            sagemaker.stop_notebook_instance(NotebookInstanceName=instance_id)
            print(
                f'Stopped SageMaker notebook instance {instance_name} due to inactivity of kernel {kernel_id}')
            break
    else:
        print(f'SageMaker notebook instance {instance_name} is still active')
