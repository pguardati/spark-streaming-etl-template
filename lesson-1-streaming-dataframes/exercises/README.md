#  Using Docker for your Exercises
The docker-compose file at the root of the repository creates 9 separate containers:
```
- Zookeeper (for Kafka)
- Kafka
- Kafka Connect with Redis Source Connector

- Spark Master
- Spark Worker

- Redis

- Banking Simulation
- Trucking Simulation
- STEDI (Application used in Final Project)
```
start command:
```
docker-compose up
```
check containers
```
docker ps
```

# Launch local services:
```
confluent local services start
sh lesson-1-streaming-dataframes/exercises/local_setup/create_master_slave.sh
ps -ef
```

# Create a hello world Spark application and submit it to the cluster
- Complete the hellospark.py python script
```
sh lesson-1-streaming-dataframes/exercises/starter/submit-spark.sh
```
- This command is using docker exec to target the container running the Spark Master, 
executing the command `spark-submit`, and passing the path to hellospark.py 
within the mounted filesystem. 
- Watch for the output at the end for the counts

# Create a Spark Streaming DataFrame with a Kafka source and write it to the console
- Complete the kafkaconsole.py python script
- Submit the application to the spark cluster:
```
submit-kakfaconsole.sh
```

# Query a temporary spark view
We've started a Spark cluster, and connected to Kafka. 
Let's do some basic queries on a DataFrame. 
Using spark.sql we will execute a query "select * from ATMVisits" and see ATM Visit data.
- Complete the atm-visits.py python script
- Submit the application to the spark cluster:
```
submit-atm-visits.sh
```