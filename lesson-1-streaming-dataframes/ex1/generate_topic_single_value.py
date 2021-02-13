from utils.utils_kafka import produce_consume_sync

produce_consume_sync(
    topic_name="test_topic",
    broker_url="PLAINTEXT://localhost:9092",
    delete_topic_after_consumption=False,
    multiple_field=False
)
