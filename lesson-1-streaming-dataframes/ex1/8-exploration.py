"""Enter in the app and generate data
Pass: Dr@gh1
"""

"""check redis content in kafka 
docker exec -it \
nd029-c2-apache-spark-and-spark-streaming-starter-master_kafka_1/ \
kafka-topics --bootstrap-server localhost:9092 --list

docker exec -it \
nd029-c2-apache-spark-and-spark-streaming-starter-master_kafka_1/ \
kafka-console-consumer --bootstrap-server localhost:9092 \
--topic "redis-server" --from-beginning
"""

"""check redis content in redis
docker exec -it nd029-c2-apache-spark-and-spark-streaming-starter-master_redis_1 redis-cli
"""
"""check elements from first (0) to last (-1)
zrange Customer 0 -1
"""
""" check other fields
keys **

1) "LoginToken"
2) "Customer"
3) "RapidStepTest"
4) "User"
"""

""" check login
zrange LoginToken 0 -1
"""