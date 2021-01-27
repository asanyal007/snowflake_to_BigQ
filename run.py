import Data_proc
import os
from google.cloud import storage

# GCP info
project_id= "onyx-sequencer-297412"
region = "us-central1"
cluster_name = "cluster-9bde"
path = "dataproc-staging-us-central1-607396110715-zl7fbquj"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="onyx-sequencer-297412-67e580bc8161.json"

# Snowlake info
user = 'INDIKA123'
password = 'Nq1dRuaV'
account = 'cz87434.us-central1.gcp'
databse = 'SNOWFLAKE_SAMPLE_DATA'

dict_info = [user,password, account, databse]


# file info
filename1 = "main.py"
filename2 = "data_transfer.py"
client = storage.Client()
bucket = client.get_bucket(path)
blob2 = bucket.blob('main.py')
blob2.upload_from_filename('main.py')
blob3 = bucket.blob('data_transfer.py')
blob3.upload_from_filename('data_transfer.py')
blob3 = bucket.blob('map.csv')
blob3.upload_from_filename('map.csv')


# Calling Job
job = '2'
Data_proc.submit_job(project_id, region, cluster_name, path, filename1, filename2, job, path, project_id, dict_info)


blob2.delete()
blob3.delete()

