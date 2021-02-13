# STEDI Ecosystem
- Log in to the STEDI application: http://localhost:4567
- Click Create New Customer, create a test customer and submit
- Click start, then add steps until you reach 30 and the timer has stopped
- Repeat this three times, and you will receive a risk score

# Analyzing the Data
connect to Redis: 
```
docker exec -it nd029-c2-apache-spark-and-spark-streaming_redis_1 redis-cli
```
```
zrange customer 0 -1
```

# Redis Server 
Create kafka consumer
```
docker exec -it nd029-c2-apache-spark-and-spark-streaming_kafka_1 
kafka-console-consumer --bootstrap-server localhost:9092 --topic redis-server
```
Back in the redis-cli, type: 
```
zadd Customer 0 "{\"customerName\":\"Sam Test\",\"email\":\"sam.test@test.com\",\"phone\":\"8015551212\",\"birthDay\":\"2001-01-03\"}"
```
In the kafka consumer terminal you will see the following payload appear in the redis-server topic:
```json
{
  "key": "__Q3VzdG9tZXI=__",
  "existType": "NONE",
  "Ch": false,
  "Incr": false,
  "zSetEntries": [
    {
      "element": "__eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==__",
      "Score": 0.0
    }
  ],
  "zsetEntries": [
    {
      "element": "eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
      "score": 0.0
    }
  ]
}

```
converted from base64
```json
{
  "key": "__Customer__",
  "existType": "NONE",
  "Ch": false,
  "Incr": false,
  "zSetEntries": [
    {
      "element": "{"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}",
      "Score": 0.0
    }__
  ],
  "zsetEntries": [
    {
      "element": "{"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}",
      "score": 0.0
    }
  ]
}
```

# Stedi Events
Whenever a customer takes an assessment, 
their risk score is generated after 4 attempts risk transmitted to `stedi-events`. 
The `stedi-events` Kafka topic has a: 
- String key 
- String value  
as a JSON object with this format:
```json
{
  "customer": "Jason.Mitra@test.com",
  "score": 7.0,
  "riskDate": "2020-09-14T07:54:06.417Z"
}

```
- Spark master and worker run as part of the docker-compose configuration
- Save the Spark startup logs for submission with your solution using the commands below:
```
docker logs nd029-c2-apache-spark-and-spark-streaming_spark_1 >& ../../spark/logs/spark-master.log
docker logs nd029-c2-apache-spark-and-spark-streaming_spark_1 >& ../../spark/logs/spark-master.log >& ../../spark/logs/spark-worker.log
```
- Create a new Kafka topic to transmit the complete risk score with birth date, 
so the data can be viewed in the STEDI application graph

# Simulation
- Log in to the STEDI application: http://localhost:4567
- Toggle to create additional customers for redis events. 
Each time you activate it, STEDI creates 30 new customers + risks after 4 minutes.
- To monitor the progress of data generated, from a terminal type: 
```
docker logs -f nd029-c2-apache-spark-and-spark-streaming-starter-master_stedi_1
```

# Deliverables
Each will connect to a kafka broker running at `kafka:19092` :

## delivery 1
`redis-server` topic: 
Write one spark script `sparkpyrediskafkastreamtoconsole.pya` 
to subscribe to the `redis-server` topic, 
base64 decode the payload, 
and deserialize the JSON to individual fields, 
then print the fields to the console. 
The data should include the birth date and email address. You will need these.

## delivery 2
`stedi-events` topic: 
Write a second spark script `sparkpyeventskafkastreamtoconsole.py` 
to subscribe to the `stedi-events` topic,
and deserialize the JSON (it is not base64 encoded) to individual fields. 
You will need the email address and the risk score.

## delivery 3
Write a spark script `sparkpykafkajoin.py` to join the customer dataframe and the customer risk dataframes,
joining on the email address. 
Create a JSON output to the newly created kafka topic you 
configured for STEDI to subscribe to that contains at least the fields below:

```json
{"customer":"Santosh.Fibonnaci@test.com",
 "score":"28.5",
 "email":"Santosh.Fibonnaci@test.com",
 "birthYear":"1963"
} 
```
- From a new terminal type: `submit-event-kafkajoin.sh` or `submit-event-kafkajoin.cmd` to submit to the cluster
- Once the data is populated in the configured kafka topic, the graph should have real-time data points
- Upload at least two screenshots of the working graph to the screenshots workspace folder 

