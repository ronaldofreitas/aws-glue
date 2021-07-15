import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

connection_mysql8_options = {
    "url": "jdbc:banco_protocol://host:porta/banco",
    "dbtable": "tabela",
    "user": "user",
    "password": "senha",
    "customJdbcDriverS3Path": "s3://glue-dependencies-teste/dependencies/mysql-connector-java-8.0.19.jar",
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"
}

# Read from JDBC databases with custom driver
df_mysql8 = glueContext.create_dynamic_frame.from_options(connection_type="mysql", connection_options=connection_mysql8_options)

# Read DynamicFrame from MySQL 5 and write to MySQL 8
#df_mysql5 = glueContext.create_dynamic_frame.from_options(connection_type="mysql", connection_options=connection_mysql5_options)
#glueContext.write_from_options(frame_or_dfc=df_mysql5, connection_type="mysql", connection_options=connection_mysql8_options)

user_spark_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://host:port/banco") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "tabela") \
    .option("user", "usuario") \
    .option("password", "senha") \
    .load()

print('total posts', user_spark_df.count())
print('mostra 1 \n')
print(user_spark_df.show(1))
print('COLLLLSSS \n')
print(user_spark_df.columns)


print('TYPESSSSS \n')
print(user_spark_df.dtypes)
print('---------- \n \n -----------')

columns_to_drop = ['post_author', 'post_date']
df2 = user_spark_df.drop(*columns_to_drop)
print('---->>>>>>>>>> novas colunas  \n \n')

print(df2.columns)
