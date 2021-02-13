import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, \
    split, expr
from pyspark.sql.types import StructField, StructType, StringType, \
    BooleanType, ArrayType, DateType

from utils.utils_kafka import produce_consume_sync, sample_stream
from utils.utils_kafka import delete_public_topic

BROKER_URL = "localhost:9092"

"""define data models"""
# TO-DO: create a StructType for the Kafka redis-server topic
# which has all changes made to Redis
redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("ch", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType())
            ])
        ))
    ]
)
# TO-DO: create a StructType for the Customer JSON that comes from Redis
customerMessage = StructType(
    [
        StructField("customer",
                    StructType([
                        StructField("customerName", StringType()),
                        StructField("email", StringType()),
                        StructField("phone", StringType()),
                        StructField("birthDay", DateType())
                    ])
                    )
    ]
)

# TO-DO: create a spark application object
# TO-DO: set the spark log level to WARN
conf = SparkConf().setMaster("spark://localhost:7077")\
    .setAppName('redis-events-explore')
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
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
df = df.selectExpr(
    "cast(value as string) value"
)

# TO-DO:; parse the single column "value" with a json object in it, like this:
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# storing them in a temporary view called RedisSortedSet
df.withColumn("value", from_json("value", redisMessageSchema)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("RedisSortedSet")

# TO-DO: execute a sql statement against a temporary view,
# which statement takes the element field
# from the 0th element in the array of structs
# and create a column called encodedCustomer
RedisSortedSet = spark.sql("""
select zSetEntries[0].element as encodedCustomer 
from RedisSortedSet 
where key is not null
""")

# TO-DO: take the encodedCustomer and decode it from base64
RedisSortedSet = RedisSortedSet.withColumn(
    "encodedCustomer",
    unbase64(RedisSortedSet.encodedCustomer).cast("string"))

# TO-DO: parse the JSON in the Customer record
# and store in a temporary view called CustomerRecords
RedisSortedSet \
    .withColumn("encodedCustomer",
                from_json("encodedCustomer", customerMessage)) \
    .select(col("encodedCustomer.*")) \
    .createOrReplaceTempView("CustomerRecords")

# TO-DO: JSON parsing will set non-existent fields to null,
# so let's select just the fields we want,
# where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql("""
select customer.* 
from CustomerRecords 
where customer.birthDay is not null
and customer.email is not null
""")

# TO-DO: from the emailAndBirthDayStreamingDF dataframe
# select the email and the birth year (using the split function)
# TO-DO: Split the birth year as a separate field from the birthday
# TO-DO: Select only the birth year and email fields
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-").
        getItem(0).alias("birthYear")
)

# TO-DO: sink the emailAndBirthYearStreamingDF dataframe to the console in append mode
query = emailAndBirthDayStreamingDF.writeStream.outputMode("append") \
    .queryName("tmp").format("console") \
    .start()
query.awaitTermination(10)
query.stop()
#
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----
