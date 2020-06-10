from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf() \
    .setAppName("pyspark-basics") \
    .setMaster("local[*]") \

spark_session = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()


