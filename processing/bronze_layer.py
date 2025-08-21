from pyspark_libraries import *

spark = SparkSession.builder.appName("Bronze").getOrCreate()