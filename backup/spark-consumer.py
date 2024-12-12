from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer

# Kafka consumer parameters
kafka_bootstrap_servers = 'localhost:9092'
topic = 'dataset-topic'

# Function to consume data from Kafka
def consume_from_kafka(topic, bootstrap_servers):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    messages = []
    for message in consumer:
        messages.append(message.value)
        if len(messages) >= 1000:  # Process in batches of 1000 messages
            yield messages
            messages = []
    if messages:
        yield messages

# Function to process data with Spark
def process_with_spark(data):
    spark = SparkSession.builder.appName("KafkaSparkConsumer").getOrCreate()
    df = spark.createDataFrame(data, "string").toDF("line")
    
    # Example MapReduce operation: Word count
    words = df.selectExpr("explode(split(line, ' ')) as word")
    word_counts = words.groupBy("word").count()
    
    word_counts.show()
    spark.stop()

# Main execution
if __name__ == "__main__":
    for messages in consume_from_kafka(topic, kafka_bootstrap_servers):
        process_with_spark(messages)