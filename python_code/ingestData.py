from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn

# Kafka Broker/topic detail
KAFKA_TOPIC_NAME_CONS = "demo17"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

# Initialize class:
if __name__ == "__main__":
    print("Real-Time Streaming Data Pipeline Started ...")

    spark = SparkSession.builder.appName("Real-Time Streaming Data Pipeline").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    '''spark = SparkSession \
        .builder \
        .appName("Real-Time Streaming Data Pipeline") \
        .master("local[*]") \
        .getOrCreate()'''

    # Read all the streaming data from server-live-status kafka topic
    liveData = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "earliest") \
        .load()
    # Function to write the streaming data from server-live-status kafka topic to hdfs datalake
    def process_row(data,id):
        data.write.mode("append").parquet("hdfs://localhost:9000/datalake/staging")
        data.printSchema()
    # For each batch that kafka producer send to the kafka server, data will be handled by process_row function
    query=liveData.writeStream.outputMode("update").foreachBatch(process_row).start()
    query.awaitTermination()
