import boto3
import requests
from datetime import datetime, timezone
import structlog

MAX_IDLE_SIZE = 30  # in minutes

structlog.configure(
    processors=[
        structlog.processors.JSONRenderer(sort_keys=True),
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt='iso'),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=structlog.threadlocal.wrap_dict(dict),
)


def lambda_handler(event, context):

    log = structlog.get_logger()

    instance_name = 'XXX'

    sagemaker = boto3.client('sagemaker')
    response = sagemaker.list_notebook_instances(NameContains=instance_name)
    instance_id = response['NotebookInstances'][0]['NotebookInstanceName']

    # retrieve the SageMaker notebook instance URL
    response = sagemaker.create_presigned_notebook_instance_url(
        NotebookInstanceName=instance_id,
        SessionExpirationDurationInSeconds=600
    )
    url = response['AuthorizedUrl']

    response = requests.get(f'{url}/api/kernels')

    # check if any of the kernels have been active recently
    for kernel in response.json():

        if kernel['execution_state'] == 'busy':
            continue

        last_activity_time_str = kernel['last_activity']
        last_activity_time = datetime.strptime(
            last_activity_time_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        current_time = datetime.now(timezone.utc)
        time_elapsed = (
            current_time - last_activity_time).total_seconds() // 60  # in minutes

        if time_elapsed > MAX_IDLE_SIZE:
            sagemaker.stop_notebook_instance(NotebookInstanceName=instance_id)
            log.warning(
                'Stopped SageMaker notebook instance due to inactivity',
                instance_name=instance_name,
                kernel_id=kernel_id,
                last_activity_time=last_activity_time_str,
                time_elapsed_minutes=time_elapsed,
            )
            break
    else:
        log.info(
            'SageMaker notebook instance is still active',
            instance_name=instance_name,
        )
