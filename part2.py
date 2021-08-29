'''

Discussions are made to achieve near real time processing capability of data that is sent from a device. Decision
Is made to  Use Kafka as a message queue. Implement a POC to read data from Kafka on a real time basis
 and save it to HDFS/ a hive table or any NOSQL DB of your choice

'''

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# Method to write rdd(tuple of 3 elements) to hive table in parquet format
def write_rdd(rdd):
    if not rdd.isEmpty():
        global spark
        df = spark.createDataFrame(rdd, schema=['device_id', 'tms', 'event'])
        df.write.saveAsTable(name='default.new_table', format='parquet', mode='append')


conf = SparkConf()
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
sc = SparkContext()
ssc = StreamingContext(sc, 10)

# DirectStream to consume data from topic
kafka_stream = KafkaUtils.createDirectStream(ssc, ['topic_name'], {'metadata.broker.list': 'localhost:2181'})

# Parse the data from topic
jsonDs = kafka_stream.map(lambda x: x[1]).map(lambda x: (x["device_id"], x["tms"], x["event"]))

# Write the data read from topic
jsonDs.foreachRDD(write_rdd)
jsonDs.foreachRDD(lambda rdd: rdd.toDF().write.saveAsTable())

ssc.start()
ssc.awaitTermination()
