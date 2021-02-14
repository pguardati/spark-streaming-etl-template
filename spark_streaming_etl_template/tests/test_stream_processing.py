import unittest
from spark_streaming_etl_template.src import utils
from spark_streaming_etl_template.scripts import stream_processing

from spark_streaming_etl_template.src.constants import BROKER_URL


class TestStreamProcessing(unittest.TestCase):
    def test_stream_processing(self):
        """Check that streams are processed without errors"""

        # 1 - kafka stage
        # generate stream source
        # in a real scenario this is done asynchronously
        utils.produce_and_consume_one_message(
            topic_name="redis-events",
            broker_url=BROKER_URL,
            field_value="redis_event"
        )
        utils.produce_and_consume_one_message(
            topic_name="customer-events",
            broker_url=BROKER_URL,
            field_value="customer_events"
        )
        utils.create_topic("intelligence-board", BROKER_URL)
        topics = utils.get_public_topics(BROKER_URL)
        print(f"available topics:{topics}")

        # 2 - spark stage
        # process multiple streams and feed the result into a kafka topic
        # in a real scenario this is deployed on a spark cluster
        stream_processing.run(
            BROKER_URL,
            topic_redis="redis-events",
            topic_customer="customer-events",
            topic_output="intelligence-board",
            output_source="kafka",
            timeout=30
        )

        # 3 - user stage
        utils.consume_all_messages(
            topic_name="intelligence-board",
            broker_url=BROKER_URL
        )
        utils.delete_public_topics(BROKER_URL)


if __name__ == "__main__":
    unittest.main()
