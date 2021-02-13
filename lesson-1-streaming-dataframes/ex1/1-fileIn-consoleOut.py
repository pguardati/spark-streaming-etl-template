""" submitting through the terminal:
docker exec -it \
nd029-c2-apache-spark-and-spark-streaming-starter-master_spark_1 \
/opt/bitnami/spark/bin/spark-submit \
/home/workspace/lesson-1-streaming-dataframes/ex1/1-fileIn-consoleOut.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# append pythonpath inside the workers
sys.path.insert(0, '/home/workspace/lesson-1-streaming-dataframes')
from ex1.constants import PROJECT_PATH

# TO-DO: create a variable with the absolute path to the text file
filepath = os.path.join(PROJECT_PATH, "ex1/Test.txt")

# TO-DO: create a Spark session
conf = SparkConf().setMaster("spark://localhost:7077").setAppName('helloworld')
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

# TO-DO: set the log level to WARN
sc = spark.sparkContext.setLogLevel('WARN')

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file
df = spark.read.text(filepath).cache()

# TO-DO: create a global variable for number of times the letter a is found
# TO-DO: create a global variable for number of times the letter b is found
numAs = df.filter(df.value.contains('a')).count()
numBs = df.filter(df.value.contains('b')).count()

# TO-DO: stop the spark application
spark.stop()
print("Final Result:")
print(numAs, numBs)
