# Cloud Function example to process file upload events
import os
from googleapiclient.discovery import build

def process_file_upload(event, context):
    """Triggered by a file upload event in the bucket. Calls the Dataflow ETL
    pipeline
    Args:
         event (dict): Event payload
         context (google.cloud.functions.Context): Metadata for the event
    """
    # Process only files in the "in/" folder
    file_name = event['name']
    if "in/" in event['name']:
        db_user, db_pass, db_host = os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_HOST")

        if not db_user or not db_pass or not db_host:
            raise Exception("Cannot execute function. No DB env vars found")

        bucket = event['bucket']
        job_name = bucket + "-upload-etl-" + event['timeCreated'].replace(":", "-")
        template_path = "gs://" + bucket + "/dataflow_templates/cars-etl.json"
        input_file = "gs://" + bucket + "/" + file_name

        payload_params = {
            "input": input_file,
            "db_user": db_user,
            "db_pass": db_pass,
            "db_host": db_host
        }

        environment_params = {"tempLocation": "gs://" + bucket + "/tmp"}

        try:
            service = build('dataflow', 'v1b3', cache_discovery=False)

            request = service.projects().locations().flexTemplates().launch(
                projectId = bucket,
                location = "us-east1",
                body = {
                    "launchParameter": {
                        "jobName": job_name,
                        "parameters": payload_params,
                        "environment": environment_params,
                        "containerSpecGcsPath": template_path
                    }
                }
            )

            response = request.execute()
            print(response)
        except Exception as e:
            print(e)
