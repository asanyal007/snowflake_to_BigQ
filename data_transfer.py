# coding: utf-8

# In[1]:


import snowflake.connector
from google.cloud import bigquery
from pyspark.sql import SparkSession
import gcsfs
from google.oauth2 import service_account
from google.api_core.exceptions import ClientError
import re
import logging
import pandas as pd
from multiprocessing.pool import ThreadPool
import datetime
import sys
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col

path = sys.argv[1]
project = sys.argv[2]
user = sys.argv[3]
password = sys.argv[4]
account = sys.argv[5]
databse = sys.argv[6]

class Recon:
    def __init__(self, source_df, target_df):
        self.source_df = source_df
        self.target_df = target_df

    def count_recon(self):
        return self.source_df.count() - self.target_df.count()

    def random_sample_check(self, guessedFraction, noOfSamples, seed):
      source_sampled = self.source_df
      target_sampled = self.target_df
      unmatched = source_sampled.subtract(target_sampled)
      return unmatched

class CreateLakes:
    def __init__(self, storage):
        self.storage = storage

    def create_deltalake(self, data, folder, filename, mode, partition_by): # only works with Spark 2.4
        if partition_by:
            data.write.partitionBy(partition_by).format("delta").mode(mode).save(self.storage+"/"+ folder + "/"+ filename)
        else:
            data.write.format("delta").mode(mode).save(self.storage + "/" + folder + "/" + filename)


spark = SparkSession \
    .builder \
    .appName("DataMigration") \
    .getOrCreate()
bucket = path
spark.conf.set('temporaryGcsBucket', bucket)

options = {
    "sfUrl": "https://{account}.snowflakecomputing.com".format(account=account),
    "sfUser": "{user}".format(user=user),
    "sfPassword": "{password}".format(password=password),
    "sfDatabase": "{databse}".format(databse=databse),
    "sfWarehouse": "COMPUTE_WH"
}

fs = gcsfs.GCSFileSystem(project=project)
with fs.open('gs://{}/map.csv'.format(path)) as f:
    Tables_to_create = pd.read_csv(f)


logging.basicConfig(filename='run.log', level=logging.INFO)


ctx = snowflake.connector.connect(
    user= user,
    password=password,
    account= account
)



sql_cols = """select t.table_schema,
       t.table_catalog,
       t.table_name
from information_schema.tables t
inner join information_schema.columns c on 
         c.table_schema = t.table_schema and c.table_name = t.table_name
where table_type = 'BASE TABLE'    
order by table_schema,
       table_name,
       ordinal_position;"""



def truncate(dataset, table):
    print("truncating {dataset}.{table}".format(dataset=dataset, table=table))
    client = bigquery.Client()
    dml_statement = (
        "TRUNCATE TABLE {dataset}.{table}".format(dataset=dataset, table=table))
    print(dml_statement)
    query_job = client.query(dml_statement)

def create_log(dataset, table):
    client = bigquery.Client()
    dml_statement = ("CREATE or REPLACE TABLE Logs.run_log3"
                        + "(dataset	STRING, "
                        +    "table	STRING, "
                        +    "status STRING, "
                        +    "start	TIMESTAMP, "
                        +    "source_count INT64, "
                        +    "target_count INT64, "
                        +    "total_unmatched_rows INT64, "
                        +    "Finish STRING)")

    query_job = client.query(dml_statement)

def update_log(dataset, table, status, start, source_count, target_count, total_unmatched_rows, Finish):
    client = bigquery.Client()
    dml_statement = (
            "UPDATE `gothic-sled-306606.Logs.run_log3` ".format(dataset=dataset, table=table)
            + "SET status = '{status}', ".format(status=status)
            + "start = '{start}', ".format(start=start)
            + "source_count = '{source_count}', ".format(source_count = source_count)
            + "target_count = '{target_count}', ".format(target_count = target_count)
            + "total_unmatched_rows ='{total_unmatched_rows}', ".format(total_unmatched_rows = total_unmatched_rows)
            + "Finish = '{Finish}' ".format(Finish=Finish)
            + "WHERE dataset = '{dataset}' and table = '{table}'".format(dataset=dataset, table=table))
    print(dml_statement)
    query_job = client.query(dml_statement)


cs = ctx.cursor()
cs.execute('USE DATABASE {database};'.format(database=databse))
cs.execute(sql_cols)
tables_columns = cs.fetchall()



def parameterize(df_schema):
    args = []
    '''for schema in df_schema['Schema']:
        for table in df_schema[df_schema['Schema'] == schema]['tables']:
            if table in Tables_to_create['SourceTab'].values and schema in Tables_to_create['Schema'].values:
                args.append((schema, table))'''
    for index, row in Tables_to_create.iterrows():
        args.append((row['Schema'], row['SourceTab']))
    return args

df_log = spark.read.format("bigquery").option('table', '{}:Logs.run_log3'.format(project)).load()

if df_log.count() > 0:
    df_rerun = pd.DataFrame()
    # Check if Log exists
    df_log_pandas = df_log.toPandas()
    df_log_pandas = df_log_pandas[df_log_pandas['status'].isin(['L','N'])]
    df_rerun['Schema'] = df_log_pandas['dataset']
    df_rerun['tables'] = df_log_pandas['table']
    args = parameterize(df_rerun)
else:
    df_schema = pd.DataFrame(tables_columns, columns=['Schema', 'Database', 'tables'])
    df_schema.drop_duplicates(subset=None, keep='first', inplace=True)
    args = parameterize(df_schema)
    truncate('Logs', 'run_log3')
    log_table = pd.DataFrame()
    log_table['dataset'] = df_schema['Schema']
    log_table['table'] = df_schema['tables']
    log_table['status'] = 'N'
    log_table['start'] = datetime.datetime.now()
    log_table['Finish'] = ''
    log_table = log_table.drop_duplicates()
    log_df = spark.createDataFrame(log_table)
    log_df.write.format('bigquery').option('table', '{}:Logs.run_log3'.format(project)).mode("append").save()

def data_transfer(val):
    truncate(val[0], val[1])
    print("Writing {}.{}".format(val[0], val[1]))
    start = datetime.datetime.now()
    end = ''

    df = spark.read.format("snowflake").options(**options).option("sfSchema",
                                                                  "{Schema}".format(Schema=str(val[0]))).option("dbtable",val[1]).load()

    update_log(dataset=val[0], table=val[1], status='L', start=start, source_count=df.count(), target_count=0,
               total_unmatched_rows=0, Finish=0)

    df.write.format('bigquery').option('table', '{}:{}.{}'.format(project, str(val[0]),
                                                                                                    str(val[1]))).mode(
        "append").save()
    df_bigq = spark.read.format("bigquery").option('table', '{}:{}.{}'.format(project, str(val[0]),
                                                                                                 str(val[1]))).load()
    end = datetime.datetime.now()


    print("Written {} to '{}:{}.{}'".format(project, df.count(), str(val[0]), str(val[1])))
    # Data migration testing
    test1 = Recon(df, df_bigq)
    unmatched = test1.random_sample_check(guessedFraction=0.1, noOfSamples=100000, seed=123).count()
    #print("Total Unmatched result between source and target for sample {} is {}".format(100000,unmatched.count()))

    if test1.count_recon() != 0 or unmatched != 0:
        update_log(dataset= val[0], table=val[1], status='E', start=start, source_count=df.count(), target_count=df_bigq.count(), total_unmatched_rows = unmatched, Finish=end)
    else:
        update_log(dataset= val[0], table=val[1], status='F', start=start, source_count=df.count(), target_count=df_bigq.count(), total_unmatched_rows = unmatched, Finish=end)



if __name__ == "__main__":
    thread_pool = ThreadPool(4)
    thread_pool.map(data_transfer, list(set(args)))




