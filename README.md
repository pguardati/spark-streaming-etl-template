# spark-streaming-etl-template
Template of a spark streaming script.
The script:
- Fetch data from 2 kafka topic
- Transform them 
- Store the result in an additional Kafka topic   

The template has been designed for the Data Streaming Nanodegree of Udacity.

## Setup

### Update PYTHONPATH 
Add the current project folder path to PYTHONPATH.
In ~/.bashrc, append:
```
PYTHONPATH=your/path/to/repo:$PYTHONPATH 
export PYTHONPATH
```
e.g.
```
PYTHONPATH=~/PycharmProjects/spark-streaming-template:$PYTHONPATH 
export PYTHONPATH
```

### Install the environment
```
conda env create -f environment.yml
conda activate spark_streaming_etl_template
```

### Run containerised Spark and Kafka
```
docker-compose up
```
### Test
A synchronous version of the script is available in `/tests`.
To execute it, run:
```
python spark_streaming_etl_template/tests/test_stream_processing.py
```
