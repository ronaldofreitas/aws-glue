from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

import sys
from awsglue.utils import getResolvedOptions

glueContext = GlueContext(SparkContext.getOrCreate())
glueJob = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueJob.init(args['JOB_NAME', args])
sparkSesion = glueContext.sparkSession

#ETL

user_spark_df = sparkSesion.read.format("jdbc") \
    .option("url", "jdbc:mysql://mysql743.umbler.com:41890/dappsxistms") \
    .option("driver", " ----- ") \
    .option("dbtable", "wp_posts") \
    .option("user", "wpdxjkas") \
    .option("password", "262dsadasd566SS65dsa") \
    .load()

print('CONTAGEMMMMMMMMMMM', user_spark_df.count())
glueJob.commit()