from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, col, udf, lag, date_add, explode, lit, unix_timestamp
from pyspark.sql.functions import month, weekofyear, dayofmonth
from pyspark.sql.types import *
from pyspark.sql.types import DateType
from pyspark.sql.dataframe import *
from pyspark.sql.window import Window
from pyspark.sql import Row


df = sqlContext.read.format('csv').options(header='true').load('GS.csv')
#df = sqlContext.read.format('csv').options(header='true', delimiter=';').load('GS.csv')

df.printSchema()

# Carregando o resultado do notebook 1
df = sqlContext.read.parquet('notebook1_resultado.parquet')

for lag_n in lags:
    wSpec = Window.partitionBy('deviceid').orderBy('date').rowsBetween(1 - lag_n, 0)
    for col_name in rolling_features:
        df = df.withColumn(col_name+'_rollingmean_'+str(lag_n), F.avg(col(col_name)).over(wSpec))
        print("Lag = %d, Column = %s" % (lag_n, col_name))

# Salvando o resultado intermediário
df.write.mode('overwrite').parquet('data_rollingmean.parquet')