"""TopicInput-TopicOutput"""
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, explode, col, unbase64, \
    base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, \
    BooleanType, IntegerType, ArrayType
from utils.utils_kafka import consume_all

# enter in spark
BROKER_URL = "localhost:9092"
topic_output = "topic_output_merged"
conf = SparkConf().setMaster("spark://localhost:7077").setAppName('helloworld')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

"""declare data structures"""
model_timestamp = StructType([
    StructField("timestamp", IntegerType())
])
model_multi_value = StructType([
    StructField("value1", StringType()),
    StructField("value2", StringType()),
])
model_single_value = StructType([
    StructField("value", StringType()),
])

"""get data from topic with multiple values"""
df_multi = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "test_topic_multi_value") \
    .option("startingOffsets", "earliest") \
    .load()
df_multi = df_multi.selectExpr(
    "cast(key as string) key",
    "cast(value as string) value"
)
# select the column to flatten
df_multi \
    .withColumn("value", from_json("value", model_multi_value)) \
    .withColumn("key", from_json("key", model_timestamp)) \
    .select(col("key.timestamp"), col("value.*")) \
    .createOrReplaceTempView("df_multi")
df_multi_processed = spark.sql("select * from df_multi")
multi_out = df_multi_processed.writeStream.outputMode("append").format(
    "console").start()
multi_out.stop()

"""get dataframe with one value"""
df_single = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "test_topic") \
    .option("startingOffsets", "earliest") \
    .load()
df_single = df_single.selectExpr(
    "cast(key as string) key",
    "cast(value as string) value"
)
df_single \
    .withColumn("value", from_json("value", model_single_value)) \
    .withColumn("key", from_json("key", model_timestamp)) \
    .select(col("key.timestamp").alias("ts_single"), col("value.*")) \
    .createOrReplaceTempView("df_single")
df_single_processed = spark.sql("select * from df_single")
single_out = df_single_processed.writeStream.outputMode("append").format(
    "console").start()
single_out.stop()

"""join processed frames"""
df_merge = df_multi_processed. \
    join(df_single_processed, expr("""
    timestamp=ts_single
    """))

# df_merge.writeStream.outputMode("append").format("console").start()
# it is noticeable the lag of the topic that emits 2 values
stream = df_merge\
    .selectExpr("cast(timestamp as string) as key",
                "to_json(struct(*)) as value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", topic_output) \
    .option("checkpointLocation", "/tmp/kafkacheckpoint_merged2") \
    .start()

consume_all(topic_output,BROKER_URL)