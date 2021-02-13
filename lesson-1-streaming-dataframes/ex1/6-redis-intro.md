# Manually Save and Read With Redis and Kafka
Note:
kafka,redis and connect dockers have to be online

redis commands:
```
docker exec -it nd029-c2-apache-spark-and-spark-streaming-starter-master_redis_1 redis-cli
```
```
keys **
zadd testkey 0 testvalue
```

consume from redis (base64 encoded name of redis tables):
```
docker exec -it \
nd029-c2-apache-spark-and-spark-streaming-starter-master_kafka_1 \
kafka-console-consumer --bootstrap-server localhost:9092 \
--topic "redis-server" --from-beginning
```
output:
```
{
  "key": "dgvzdgtleq==",
  "existtype": "none",
  "ch": false,
  "incr": false,
  "zsetentries": [
    {
      "element": "dgvzdhzhbhvl",
      "score": 2.0
    }
  ],
  "zsetentries": [
    {
      "element": "dgvzdhzhbhvl",
      "score": 2.0
    }
  ]
}
```

to decode the table:
```
echo  "dgvzdgtleq==" | base64 -d
```

