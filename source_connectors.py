
class db_connectors:

    def snowflake_jdbc_read(self, options, dbtable, Schema, spark):
        df = spark.read.format("snowflake").options(**options).option("sfSchema",Schema).option("dbtable", dbtable).load()
        return df

    def bigquery_jdbc_read(self, spark, project, dataset, table ):
        df = spark.read.format('bigquery').option('table', '{}:{}.{}'.format(project, dataset,table)).load()
        return df

    def postgresql_jdbc_read(self, spark, dbtable, username, password):
        df = spark.read.format("jdbc").option("url", "jdbc:postgresql:dbserver").option("dbtable", dbtable).option("user", username).option("password", password).load()
        return df

    def mssql_jdbc_read(self,spark, url, table_name, username, password):
        df = spark.read \
                    .format("jdbc")\
                    .option("url", url)\
                    .option("dbtable", table_name)\
                    .option("user", username)\
                    .option("password", password)\
                    .load()

        return df
    # write
    def snowflake_jdbc_write(self, options, dbtable, Schema, df, mode):
        try:
            df.write.format("snowflake").\
                options(**options).\
                option("sfSchema",Schema).\
                option("dbtable", dbtable).mode(mode).save()
            return "saved {}".format(table)
        except Exception as e:
            return e

    def bigquery_jdbc_write(self, df, project, dataset, table, mode ):
        try:
            df.write.format('bigquery').\
                option('table', '{}:{}.{}'.format(project, dataset,table)).\
                mode(mode).save()
            return "saved {}".format(table)
        except Exception as e:
            return e

    def mssql_jdbc_write(self,df, url, table_name, username, password, mode):
        try:
            df.write \
            .format("jdbc")\
            .option("url", url)\
            .option("dbtable", table_name)\
            .option("user", username)\
            .option("password", password)\
            .mode(mode)\
            .save()
            return "saved {}".format(table_name)
        except Exception as e:
            return e
            

        










