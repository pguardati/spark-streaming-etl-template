"""TopicInput-TopicOutput"""
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from utils.utils_kafka import consume_all

BROKER_URL = "localhost:9092"
topic_output = "topic_output"

# TO-DO: create a Spark session
conf = SparkConf().setMaster("spark://localhost:7077").setAppName('helloworld')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "test_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# input from topic
df = df.selectExpr(
    "cast(key as string) timestamp",
    "cast(value as string) iteration"
)
# create a temporary table
df.createOrReplaceTempView("df")
# apply sql transformation (null in this case)
df = spark.sql("select * from df")
# output to the topic
df = df.selectExpr(
    "cast(timestamp as string) as key",
    "cast(timestamp as string) as value"
)

# sink the transformed batch into the console
stream = df.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", topic_output) \
    .option("checkpointLocation", "/tmp/kafkacheckpoint1") \
    .start()

consume_all(topic_output, BROKER_URL)
