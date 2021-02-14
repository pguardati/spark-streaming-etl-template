import os
PROJECT_NAME = 'spark_streaming_etl_template'
REPOSITORY_PATH = os.path.realpath(__file__)[
                  :os.path.realpath(__file__).find(PROJECT_NAME)]
PROJECT_PATH = os.path.join(REPOSITORY_PATH, PROJECT_NAME)
BROKER_URL = "PLAINTEXT://localhost:9092"
