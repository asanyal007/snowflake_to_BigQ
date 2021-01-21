from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import re
import os
import logging
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/aritra/PycharmProjects/SF_to_BQ/enhanced-idiom-287811-7deab48e37f8.json"

def submit_job(project_id, region, cluster_name, bucket_name, filename1, filename2, job, path, project, dict_info):
    # Create the job client.
    job_client = dataproc.JobControllerClient(client_options={
        'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)
    })

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    if job == '1':
        print("Creating Tables and Datasets!")
        job1 = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": "gs://{}/{}".format(bucket_name, filename1),
                'args': [dict_info[0],dict_info[1], dict_info[2], dict_info[3], project, path]
            },
        }
        operation = job_client.submit_job_as_operation(
            request={"project_id": project_id, "region": region, "job": job1}
        )
        response = operation.result()

        # Dataproc job output gets saved to the Google Cloud Storage bucket
        # allocated to the job. Use a regex to obtain the bucket and blob info.
        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output = (
            storage.Client()
                .get_bucket(matches.group(1))
                .blob(f"{matches.group(2)}.000000000")
                .download_as_string()
        )

        print(f"Job finished successfully: {output}")
        for t in output.decode().split(r'\n'):
            logging.warning(t)
    else:
        # Data Transfer job
        print("Trnasfering data!")
        job2 = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": "gs://{}/{}".format(bucket_name, filename2),
                "args" : [path, project, dict_info[0], dict_info[1], dict_info[2], dict_info[3]]
            },
        }
        operation = job_client.submit_job_as_operation(
            request={"project_id": project_id, "region": region, "job": job2}
        )
        response = operation.result()

        # Dataproc job output gets saved to the Google Cloud Storage bucket
        # allocated to the job. Use a regex to obtain the bucket and blob info.
        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        output = (
            storage.Client()
                .get_bucket(matches.group(1))
                .blob(f"{matches.group(2)}.000000000")
                .download_as_string()
        )

        print(f"Job finished successfully: {output}")
        for t in output.decode().split(r'\n'):
            logging.warning(t)

