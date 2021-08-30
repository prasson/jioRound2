from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf()
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

# streaming dataframe to read from topic(has key, value, timestamp, timestamp type columns .. etc)
ds_app_stream_data = spark.readStream.format("kafka").option("kafka.bootstrap.servers", 'ip_address:port_number').\
    option("subscribe", "topic_name").option("startingOffsets", "latest").load()

# Reading the value from above dataframe & converting the byte array type to string, timestamp - to know when a particular data was pushed
df_app_str_data = ds_app_stream_data.selectExpr("CAST(value AS String)", "timestamp")

# schema for the app data
schema = StructType().\
    add("device_id", StringType()).\
    add("tms", StringType()).\
    add("event", StringType())

# Extract data(values) from json as per the specified schema
df_app_data_with_schema = df_app_str_data.\
    select(from_json(col("value"), schema).alias("app_data"), "timestamp")

# dataframe to fetch device_id, tms, event, timestamp 
df_app_data = df_app_data_with_schema.select("app_data.*", "timestamp")

# write the dataframes to hdfs location in append mode
df_app_data.writeStream.format("json").outputMode("append").option("path", '/hdfs_location').\
    start().awaitTermination()
