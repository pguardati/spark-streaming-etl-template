import json
import time

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic


def get_public_topics(broker_url):
    """Get the name of the public kafka topics
    A public topic is a topic that has been created by a user
    """
    client = AdminClient({"bootstrap.servers": broker_url})
    topic_list = client.list_topics(timeout=5).topics
    public_topics = []
    for topic in topic_list.keys():
        if not topic.startswith("_") and \
                not topic.startswith("connect") and \
                not topic.startswith("default"):
            public_topics.append(topic)
    return public_topics


def create_topic(topic_name, broker_url):
    """Create a kafka topic"""
    if topic_name not in get_public_topics(broker_url):
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
        client = AdminClient({"bootstrap.servers": broker_url})
        new_topic_state = client.create_topics([topic])
        print(f"topic {topic_name} created")


def delete_public_topics(broker_url):
    "Delete all the public kafka topics"
    print("deleting public topics..")
    client = AdminClient({"bootstrap.servers": broker_url})
    clean_iteration = 0
    public_topics = get_public_topics(broker_url)
    while public_topics:
        new_topic_state = client.delete_topics(public_topics)
        public_topics = get_public_topics(broker_url)
        print(
            f"current topics after {clean_iteration} clean iterations: {public_topics}")
        clean_iteration += 1
    print("public topics deleted")


def get_message(i, field_value):
    """Get a json kafka message

    Args:
        i(int): integer that makes the message unique
        field_value(str): name of the type of message

    Returns:
        dict
    """
    if field_value == "single":
        message_value = {"value": f"mockup {i}"}
    elif field_value == "multi":
        message_value = {"value1": f"mockup {i}",
                         "value2": f"mockup {-i}"}
    elif field_value == "redis_event":
        message_value = {
            "key": "__Q3VzdG9tZXI=__",
            "existType": "NONE",
            "Ch": False,
            "Incr": False,
            "zSetEntries": [
                {
                    "element": "__eyJjdXN0b21lck5hbWUiOiJTYW"
                               "0gVGVzdCIsImVtYWlsIjoic2FtLn"
                               "Rlc3RAdGVzdC5jb20iLCJwaG9uZSI6"
                               "IjgwMTU1NTEyMTIiLCJiaXJ0aERheS"
                               "I6IjIwMDEtMDEtMDMifQ==__",
                    "Score": i
                }
            ],
            "zsetEntries": [
                {
                    "element": "eyJjdXN0b21lck5hbWUiOiJTYW0gV"
                               "GVzdCIsImVtYWlsIjoic2FtLnRlc3"
                               "RAdGVzdC5jb20iLCJwaG9uZSI6Ijg"
                               "wMTU1NTEyMTIiLCJiaXJ0aERheSI6"
                               "IjIwMDEtMDEtMDMifQ==",
                    "score": i
                }
            ]
        }
    elif field_value == "customer_events":
        message_value = {
            "customer": "sam.test@test.com",
            "score": i,
            "riskDate": "2020-09-14T07:54:06.417Z"
        }

    return message_value


def produce_and_consume_one_message(
        topic_name,
        broker_url,
        delete_topic_after_consumption=False,
        field_value="single"
):
    """Produce and consume a message in a kafka topic
    """
    print("create topic, producer and consumer")
    client = AdminClient({"bootstrap.servers": broker_url})
    producer = Producer({"bootstrap.servers": broker_url})
    consumer = Consumer({
        "bootstrap.servers": broker_url,
        "group.id": "0",
        "auto.offset.reset": "earliest"
    })
    create_topic(topic_name, broker_url)

    print(f"testing production and consumption of data from {topic_name}")
    # produce
    message_value = get_message(0, field_value)
    producer.produce(
        topic_name,
        key=json.dumps({"timestamp": int(time.time())}),
        value=json.dumps(message_value)
    )
    producer.flush()
    # consume
    consumer.subscribe([topic_name])
    mess = consumer.poll(timeout=10)
    consumer.close()

    # delete config and check result
    if delete_topic_after_consumption:
        delete_public_topics(broker_url)

    if mess:
        print(json.loads(mess.value()))
        print(json.loads(mess.key()))
    else:
        raise Exception("no message received by the consumer")


def produce_and_consume_synchronously(
        topic_name,
        broker_url,
        delete_topic_after_consumption=False,
        field_value="single"
):
    """Produce and consume kafka messages continuously"""
    try:
        print("create topic, producer and consumer")
        client = AdminClient({"bootstrap.servers": broker_url})
        producer = Producer({"bootstrap.servers": broker_url})
        consumer = Consumer({
            "bootstrap.servers": broker_url,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        })
        create_topic(topic_name, broker_url)
        consumer.subscribe([topic_name])

        i = 0
        while True:
            message_value = get_message(i, field_value)
            # produce
            producer.produce(
                topic_name,
                key=json.dumps({"timestamp": int(time.time())}),
                value=json.dumps(message_value)
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
            delete_public_topics(broker_url)


def consume_all_messages(topic_name, broker_url):
    """Consume all the messages available in a kafka topic

    Args:
        topic_name(str): name of a topic
        broker_url(str): name of a kafka broker
    """
    consumer = Consumer({
        "bootstrap.servers": broker_url,
        "group.id": "0",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([topic_name])
    # print every available message
    mess = consumer.poll(timeout=10)
    i = 0
    while mess or i < 10:
        if not mess:
            break
        print(mess.key(), mess.value())
        mess = consumer.poll(timeout=1)
        i += 1
    print(f"printed {i} streaming messages")
    consumer.close()


def sample_data_stream(spark, df, timeout=10):
    """Collect a batch of data from a streaming source

    Args:
        spark(SparkSession): spark session object
        df(spark.DataFrame): spark streaming dataframe
        timeout(int): second to wait before closing the stream

    Returns:
        pd.DataFrame
    """
    query = df.writeStream.outputMode("append") \
        .queryName("tmp").format("memory") \
        .start()
    query.awaitTermination(timeout)
    query.stop()
    return spark.sql("select * from tmp").toPandas()
