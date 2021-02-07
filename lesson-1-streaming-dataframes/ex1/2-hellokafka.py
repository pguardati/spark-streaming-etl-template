""" submitting through the terminal:
docker exec -it \
nd029-c2-apache-spark-and-spark-streaming-starter-master_spark_1 \
/opt/bitnami/spark/bin/spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
/home/workspace/lesson-1-streaming-dataframes/ex1/2-hellokafka.py \
| tee spark/logs/kafkaconsole.log
"""

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# TO-DO: create a Spark session
conf = SparkConf().setMaster("spark://localhost:7077").setAppName('helloworld')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# extract batch from the topic
df = df.selectExpr(
    "cast(key as string) key",
    "cast(value as string) value"
)
df.writeStream.start()

# keeps printing every new batch until termination
stream=df.writeStream.outputMode("append").format(
    "console").start()
# stream.awaitTermination()