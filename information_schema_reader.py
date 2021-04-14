import data_injector
import snowflake.connector
class information_schema:
    def snowflake_info(self,database, user, password, account):

        ctx = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
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
        cs = ctx.cursor()
        cs.execute('USE DATABASE {database};'.format(database=database))
        cs.execute(sql_cols)
        tables_columns = cs.fetchall()

        return tables_columns

    def mssql_info(self,spark, server,database, username, password):
        info = data_injector.reader.read_mssql(spark, server,database, username, password, table_name="INFORMATION_SCHEMA.COLUMNS" )
        return info


if __name__=='__main__':
    user = 'INDIKA123'
    password = 'Nq1dRuaV'
    account = 'cz87434.us-central1.gcp'
    databse = 'SNOWFLAKE_SAMPLE_DATA'
    info = information_schema.snowflake_info(databse=databse, user=user, password=password, account=account)
    print(info)

