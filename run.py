import Data_proc
import os
from google.cloud import storage

# GCP info
project_id= "gothic-sled-306606"
region = "us-central1"
cluster_name = "cluster-0503"
path = "dataproc-staging-us-central1-780791141010-l5dhlhuq"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gothic-sled-306606-7cab126977a1.json"

# Snowlake info
user = 'WNS123'
password = 'Nq1dRuaV'
account = 'dt21316.us-central1.gcp'
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
job = ['1', '2']
for j in job:
    Data_proc.submit_job(project_id, region, cluster_name, path, filename1, filename2, j, path, project_id, dict_info)


blob2.delete()
blob3.delete()

