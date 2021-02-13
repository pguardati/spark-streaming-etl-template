import os

os.environ['PYSPARK_SUBMIT_ARGS'] = """
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  pyspark-shell
"""
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, \
    col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, \
    StringType, BooleanType, ArrayType, DateType, FloatType

BROKER_URL = "localhost:9092"
topic_output = "stedi-risk"

"""schema models"""
# TO-DO: create a StructType for the Kafka redis-server topic
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
# TO-DO: create a StructType for the Kafka stedi-events topic
# which has the Customer Risk JSON that comes from Redis
stediEvent = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

# TO-DO: create a spark application object
# TO-DO: set the spark log level to WARN
conf = SparkConf().setMaster("spark://localhost:7077") \
    .setAppName('consume-process-produce')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
sc = spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the spark application object,
# read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic
# including those that were published before you started the spark stream
df_redis_server = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING 
df_redis_server = df_redis_server.selectExpr(
    "cast(value as string) value"
)

# TO-DO:; parse the single column "value" with a json object:
df_redis_server.withColumn("value", from_json("value", redisMessageSchema)) \
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

# TO-DO: take the encodedCustomer column which is base64 encoded :
# with this JSON format: {"customerName":"Sam Test",
# "email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
RedisSortedSet = RedisSortedSet.withColumn(
    "encodedCustomer",
    unbase64(RedisSortedSet.encodedCustomer).cast("string"))

# TO-DO: parse the JSON in the Customer
# record and store in a temporary view called CustomerRecords
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

# TO-DO: Split the birth year as a separate field from the birthday
# TO-DO: Select only the birth year and email fields as a
# new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-").
        getItem(0).alias("birthYear")
)

"""stedi-events"""
# TO-DO: using the spark application object,
# read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic
# including those that were published before you started the spark stream
df_stedi_events = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER_URL) \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# TO-DO: cast the value column in the streaming dataframe as a STRING
df_stedi_events = df_stedi_events.selectExpr("cast(value as string) value")

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
# storing them in a temporary view called CustomerRisk
df_stedi_events.withColumn("value", from_json("value", stediEvent)) \
    .select(col("value.*")) \
    .createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view,
# selecting the customer and the score from the temporary view,
# creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("""
select customer, score 
from CustomerRisk
""")


"""join stedi-events and redis-server"""
# TO-DO: join the streaming dataframes on the email address
# to get the risk score and the birth year in the same dataframe
df_merge = customerRiskStreamingDF. \
    join(emailAndBirthYearStreamingDF, expr("""
    email=customer
    """))

# TO-DO: sink the joined dataframes to a new kafka topic
# to send the data to the STEDI graph application
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","
# score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}
stream = df_merge \
    .selectExpr("cast(customer as string) as key",
                "to_json(struct(*)) as value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", topic_output) \
    .option("checkpointLocation", "/tmp/kafkacheckpoint_merged_final") \
    .start()
# stream.awaitTermination(20)
# stream.stop()