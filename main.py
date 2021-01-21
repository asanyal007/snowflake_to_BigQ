# pip install --upgrade snowflake-connector-python , pip install gcsfs
import snowflake.connector
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import ClientError
import re
import pandas as pd
import sys
import gcsfs
import math
from google.cloud import storage

user = sys.argv[1]
password = sys.argv[2]
account = sys.argv[3]
database = sys.argv[4]
project = sys.argv[5]
path = sys.argv[6]

def create_table(user, password, account, database, project, path):

    import snowflake.connector
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from google.api_core.exceptions import ClientError
    import re
    import pandas as pd

    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )
    fs = gcsfs.GCSFileSystem(project=project)
    with fs.open('gs://{}/map.csv'.format(path)) as f:
        Tables_to_create = pd.read_csv(f)
    #Tables_to_create = pd.read_csv('gs://dataproc-staging-us-central1-568505890093-n8cqd2jx/map.csv')
    data_types = {
        'INTEGER': 'INT64',
        'DOUBLE': 'FLOAT64',
        'NUMBER': 'NUMERIC',
        'VARCHAR': 'STRING',
        'TEXT': 'STRING',
        'VARIANT': 'STRING'
    }

    mode = {'NO': 'NOT NULL', 'YES': ''}


    sql_cols = """select t.table_schema,
           t.table_catalog,
           t.table_name,
           c.column_name,
           c.data_type, 
           c.is_nullable
    from information_schema.tables t
    inner join information_schema.columns c on 
             c.table_schema = t.table_schema and c.table_name = t.table_name
    where table_type = 'BASE TABLE'    
    order by table_schema,
           table_name,
           ordinal_position;"""

    cs = ctx.cursor()
    cs.execute('USE DATABASE {database};'.format(database=database))
    cs.execute(sql_cols)
    tables_columns = cs.fetchall()

    df_schema = pd.DataFrame(tables_columns, columns=['Schema', 'Database', 'tables', 'cols', 'Dtype', 'Isnull'])
    df_schema.drop_duplicates(subset=None, keep='first', inplace=True)
    df_schema_bigQ = df_schema.replace({"Dtype": data_types, "Isnull": mode})

    # # Create all the datasets

    done = []
    client = bigquery.Client()
    for d in set(df_schema_bigQ['Schema']):
        if d not in done:
            dataset_id = "{}.{}".format(client.project, d)
            dataset = bigquery.Dataset(dataset_id)
            dataset = client.create_dataset(dataset, exists_ok=True)
        done.append(d)
        print("Created Dataset {}".format(dataset_id))

    df_schema_bigQ['concat'] = df_schema_bigQ[['cols', 'Dtype', 'Isnull']].apply(lambda x: ' '.join(x), axis=1)

    df = df_schema_bigQ.groupby(['Schema', 'tables'])['concat'].apply(lambda x: ", ".join(x)).reset_index()

    # # Execute all DDLs


    done = []
    for schema in df['Schema']:
        for table in df[df['Schema'] == schema]['tables']:
            if table in Tables_to_create['SourceTab'].values and "{}.{}".format(schema, table) not in done:
                table_id = "{}.{}".format(schema, table)
                columns = str(df[(df['Schema'] == schema) & (df['tables'] == table)]['concat'].values[0])
                partition_col = Tables_to_create[Tables_to_create['SourceTab']==table]['ParitionCol'].values[0]
                DDL_Partition = "CREATE OR REPLACE TABLE {table_id} ({columns}) PARTITION BY {partition_col}".format(
                    table_id=table_id, columns=columns, partition_col=partition_col)
                DDL = "CREATE OR REPLACE TABLE {table_id} ({columns})".format(table_id=table_id, columns=columns)
                print(DDL)
                if not str(partition_col) == 'nan':
                    try:
                        query_job = client.query(DDL_Partition)
                        results = query_job.result()
                    except ClientError as err:
                        error_dic = err.__dict__['_errors']
                        errors = error_dic[0]['message']
                        print(errors)
                    print("Created {} with partition on {}".format(table_id, partition_col))
                    if "{}.{}".format(schema, table) not in done:
                        done.append("{}.{}".format(schema, table))
                else:
                    try:
                        query_job = client.query(DDL)
                        results = query_job.result()
                    except ClientError as err:
                        error_dic = err.__dict__['_errors']
                        errors = error_dic[0]['message']
                        print(errors)
                    if "{}.{}".format(schema, table) not in done:
                        done.append("{}.{}".format(schema, table))
            else:
                print("Skipped {}".format(table))



create_table(user, password, account, database , project, path)