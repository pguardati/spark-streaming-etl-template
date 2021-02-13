import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, \
    col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, \
    StringType, BooleanType, ArrayType, DateType

from utils.utils_kafka import produce_consume_sync
from utils.utils_kafka import delete_public_topic

BROKER_URL = "localhost:9092"
topic_input = "redis-server"

# declare the schema the produced kafka message
customerLocationSchema = StructType([
    StructField("accountNumber", StringType()),
    StructField("location", StringType()),
])

# declare the standard schema between redis and kafka connect
redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType())
            ])
        ))
    ]
)

# enter in spark
conf = SparkConf().setMaster("spark://localhost:7077").setAppName('helloworld')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

# get a stream of the input topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", topic_input) \
    .option("startingOffsets", "earliest") \
    .load()

df = df.selectExpr(
    "cast(key as string) key",
    "cast(value as string) value"
)
df.withColumn("value", from_json("value", redisMessageSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("df_redis")

# select key and customer location
df_redis_sel = spark.sql("""
select key, 
zSetEntries[0].element as customerLocation
from df_redis 
""")
# decode location from base64
df_redis_sel = df_redis_sel.withColumn(
    "customerLocation",
    unbase64(df_redis_sel.customerLocation).cast("string"))

# flatten location fields
df_redis_sel.withColumn(
    "customerLocation",
    from_json("customerLocation", customerLocationSchema)). \
    select(col("customerLocation.*")). \
    createOrReplaceTempView("CustomerLocation")

df_redis_processed = spark.sql("select * from CustomerLocation where location is not null")
df_redis_processed.writeStream.outputMode("append").format(
    "console").start().stop()

# TODO: cannot display output properly

# last step: output to kafka
# sumup: input from redis, select + process, output to kafka