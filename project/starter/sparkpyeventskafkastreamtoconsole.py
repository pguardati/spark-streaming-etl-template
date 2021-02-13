import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, \
    split, expr
from pyspark.sql.types import StructField, StructType, StringType, \
    BooleanType, ArrayType, DateType, FloatType

from utils.utils_kafka import produce_consume_sync, sample_stream
from utils.utils_kafka import delete_public_topic

BROKER_URL = "localhost:9092"

"""define data models"""
# TO-DO: using the spark application object,
# read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic
# including those that were published before you started the spark stream
conf = SparkConf().setMaster("spark://localhost:7077").setAppName(
    'stedi-events-explore')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the spark application object,
# read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic
# including those that were published before you started the spark stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
df = df.selectExpr("cast(value as string) value")

# TO-DO: parse the JSON from the single column "value"
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
# storing them in a temporary view called CustomerRisk
stediEvent = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)
df.withColumn("value", from_json("value", stediEvent)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view,
# selecting the customer and the score from the temporary view,
# creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("""
select customer, score 
from CustomerRisk
""")

# TO-DO: sink the customerRiskStreamingDF
# dataframe to the console in append mode
query = customerRiskStreamingDF.writeStream.outputMode("append") \
    .queryName("tmp").format("console") \
    .start()
query.awaitTermination(10)
query.stop()

# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----

