"""TopicInput-TopicOutput"""
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, \
    split, expr
from pyspark.sql.types import StructField, StructType, StringType, \
    BooleanType, ArrayType

BROKER_URL = "localhost:9092"
topic_output = "topic_output_multi_value"

# enter in spark
conf = SparkConf().setMaster("spark://localhost:7077").setAppName('helloworld')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "test_topic_multi_value") \
    .option("startingOffsets", "earliest") \
    .load()

# define input shape of a message
df = df.selectExpr(
    "cast(key as string) key",
    "cast(value as string) value"
)
# define how to parse the message (json to df)
message_schema = StructType([
    StructField("value1", StringType()),
    StructField("value2", StringType()),
])
# select the column to flattern
df.withColumn("value", from_json("value", message_schema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("df")
# process the column
df_processed = spark.sql("select * from df")
# output to console
df_processed.writeStream.outputMode("append").format("console").start()
