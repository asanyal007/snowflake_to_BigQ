import source_connectors
import pandas as pd
import data_injector
import information_schema_reader
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("DataMigration_Azure") \
    .getOrCreate()

server_name = 'test-wns'
database_name = "SNOWFLAKE_SAMPLE_DATA"
username = "aritra"
password = "Nq1dRuaV"
mode = "overwrite"

df_info = information_schema_reader.information_schema.mssql_info(spark,server_name,database_name,username, password)
tables_details = df_info[df_info['TABLE_SCHEMA']=='dbo'].select("TABLE_CATALOG", "TABLE_SCHEMA","TABLE_NAME").distinct()



pd_table_details = tables_details.toPandas()

pd_table_details.to_csv("azure_schema/azure_table_schema.csv")

pd_table_details_mod = pd.read_csv("azure_schema/azure_table_schema.csv")

for index, row in pd_table_details_mod.iterrows():
    table_name = row['TABLE_SCHEMA']+"."+row['TABLE_NAME']
    df = data_injector.reader.read_mssql(spark, server_name, database_name, username, password, table_name)
    data_injector.injector.inject_mssql(df,server_name, database_name,username,password,table_name, mode)

