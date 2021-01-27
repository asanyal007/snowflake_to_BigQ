import source_connectors
import data_injector
import information_schema_reader
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("DataMigration_Azure") \
    .getOrCreate()

server = 'test-wns'
database = "SNOWFLAKE_SAMPLE_DATA"
username = "aritra"
password = "Nq1dRuaV"

df_info = information_schema_reader.information_schema.mssql_info(spark,server,database,username, password)
tables_details = df_info[df_info['TABLE_SCHEMA']=='dbo'].select("TABLE_CATALOG", "TABLE_SCHEMA","TABLE_NAME").distinct()

