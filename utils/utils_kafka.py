import json
import time

from confluent_kafka import TopicPartition, Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic


def create_topic(topic_name, client):
    if topic_name not in get_public_topics(client):
        topic = NewTopic(
            topic=topic_name,
            num_partitions=1,
            replication_factor=1,
            config={
                "cleanup.policy": "compact",
                "compression.type": "lz4",
                "delete.retention.ms": "100",
                "file.delete.delay.ms": "100"
            }
        )
        new_topic_state = client.create_topics([topic])
        print(f"topic {topic_name} created")


def check_offset(consumer, topic_name):
    topic = consumer.list_topics(topic=topic_name)
    partitions = [TopicPartition(topic_name, partition)
                  for partition in
                  list(topic.topics[topic_name].partitions.keys())]
    offset_per_partition = consumer.position(partitions)
    return offset_per_partition


def get_public_topics(client):
    topic_list = client.list_topics(timeout=5).topics
    public_topics = []
    for topic in topic_list.keys():
        if not topic.startswith("_") and \
                not topic.startswith("connect") and \
                not topic.startswith("default"):
            public_topics.append(topic)
    return public_topics


def delete_public_topic(broker_url):
    print("deleting public topics..")
    client = AdminClient({"bootstrap.servers": broker_url})
    clean_iteration = 0
    public_topics = get_public_topics(client)
    while public_topics:
        new_topic_state = client.delete_topics(public_topics)
        public_topics = get_public_topics(client)
        print(
            f"current topics after {clean_iteration} clean iterations: {public_topics}")
        clean_iteration += 1
    print("public topics deleted")


def check_produce_consume(
        topic_name,
        broker_url,
        delete_topic_after_consumption=False
):
    """Send and Receive one kafka message
    To check that the topic exist and that data are stored inside it
    ```
        docker exec -it \
        nd029-c2-apache-spark-and-spark-streaming-starter-master_kafka_1/ \
        kafka-topics --bootstrap-server localhost:9092 --list
    ```
    ```
        docker exec -it \
        nd029-c2-apache-spark-and-spark-streaming-starter-master_kafka_1/ \
        kafka-console-consumer --bootstrap-server localhost:9092 \
        --topic "test_topic" --from-beginning
    ```
    """
    print("create topic, producer and consumer")
    client = AdminClient({"bootstrap.servers": broker_url})
    producer = Producer({"bootstrap.servers": broker_url})
    consumer = Consumer({
        "bootstrap.servers": broker_url,
        "group.id": "0",
        "auto.offset.reset": "earliest"
    })
    create_topic(topic_name, client)

    print(f"testing production and consumption of data from {topic_name}")
    # produce
    message = {
        "key": {"timestamp": int(time.time())},
        "value": {"value": "mockup"}
    }
    producer.produce(
        topic_name,
        key=json.dumps(message["key"]),
        value=json.dumps(message["value"])
    )
    producer.flush()
    # consume
    consumer.subscribe([topic_name])
    mess = consumer.poll(timeout=10)
    consumer.close()

    # delete config and check result
    if delete_topic_after_consumption:
        delete_public_topic(broker_url)

    if mess:
        print(json.loads(mess.value()))
        print(json.loads(mess.key()))
    else:
        raise Exception("no message received by the consumer")


def produce_consume_sync(
        topic_name,
        broker_url,
        delete_topic_after_consumption=False
):
    """produce and consume kafka messages in an infinite loop"""
    try:
        print("create topic, producer and consumer")
        client = AdminClient({"bootstrap.servers": broker_url})
        producer = Producer({"bootstrap.servers": broker_url})
        consumer = Consumer({
            "bootstrap.servers": broker_url,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        })
        create_topic(topic_name, client)
        consumer.subscribe([topic_name])

        i = 0
        while True:
            # produce
            producer.produce(
                topic_name,
                key=json.dumps({"timestamp": int(time.time())}),
                value=json.dumps({"value": f"mockup {i}"})
            )
            producer.flush()
            # consume
            mess = consumer.poll(timeout=0.1)
            if mess:
                print(json.loads(mess.value()))
            # update
            i += 1
            time.sleep(0.1)

    except KeyboardInterrupt:
        consumer.close()
        if delete_topic_after_consumption:
            delete_public_topic(broker_url)
