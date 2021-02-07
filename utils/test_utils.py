from utils.utils_kafka import check_produce_consume

broker_url = "PLAINTEXT://localhost:9092"
topic_name = "test_topic"
delete_topic_after_consumption = True

# test 1
check_produce_consume(
    topic_name,
    broker_url,
    delete_topic_after_consumption)
