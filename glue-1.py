from datetime import datetime
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

glue_db = 'banco-virtual'
glue_table = 'input'
s3_write_path = 's3://teste-glue-etl/output/'

# extract
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(
    database = glue_db,
    table_name = glue_table
)

data_frame = dynamic_frame_read.toDF()

# transform
data_frame_aggregated = data_frame.groupby("date").agg(
    f.mean(f.col("high")).alias("avg_high"),
    f.mean(f.col("low")).alias("avg_low")
)

data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("avg_high"))

# load
data_frame_aggregated = data_frame_aggregated.repartition(1)
dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")

glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path
    },
    format = "csv"
)
