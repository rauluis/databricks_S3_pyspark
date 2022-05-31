import imp
import pandas as pd
read = pd.read_csv('test.csv')

##pip install pyspark
import pyspark

from pysparck.sql import SparkSession
spark = SparkSession.builder.appName('Practice').getOrCreate()

spark

df_pyspark = spark.read.csv('test.csv')

df_pyspark 

df_pyspark.show()

df_pyspark = spark.read.option('header','true').csv('test.csv')

df_pyspark.show()

type(df_pyspark)

df_pyspark.head(2)
df_pyspark.printSchema()