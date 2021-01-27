
import source_connectors
source_tables = []
target_tables = []

class injector:
    def inject_mssql(self,df,server_name, database_name,username,password,table_name, mode):
        server_name = "jdbc:sqlserver://{}.database.windows.net".format(server_name)
        url = server_name + ";" + "databaseName=" + database_name + ";"
        res = source_connectors.db_connectors.mssql_jdbc_write(df=df, url=url,table_name=table_name,mode =mode,
                                                           username=username, password=password)

        return res

    def inject_bigqury(self, df, project, dataset, table, mode):
        res = source_connectors.db_connectors.bigquery_jdbc_write(self, df, project, dataset, table, mode)
        return res

class reader:
    def read_mssql(self, spark, server_name, database_name, username, password, table_name):
        server_name = "jdbc:sqlserver://{}.database.windows.net".format(server_name)
        url = server_name + ";" + "databaseName=" + database_name + ";"
        res = source_connectors.db_connectors.mssql_jdbc_read(spark, url, table_name, username, password)
        return res

    def read_bigqury(self, spark, project, dataset, table, mode):
        res = source_connectors.db_connectors.bigquery_jdbc_read(self, spark, project, dataset, table)
        return res

    def read_snowflake(self, spark, account, user, password, databse, warehouse, dbtable, Schema):
        options = {
            "sfUrl": "https://{account}.snowflakecomputing.com".format(account=account),
            "sfUser": "{user}".format(user=user),
            "sfPassword": "{password}".format(password=password),
            "sfDatabase": "{databse}".format(databse=databse),
            "sfWarehouse": warehouse
        }
        res = source_connectors.db_connectors.snowflake_jdbc_read(options, dbtable, Schema, spark)
        return res

    def read_postgresql(self, spark, dbtable, username, password):
        res = source_connectors.db_connectors.postgresql_jdbc_read(spark, dbtable, username, password)
        return res









