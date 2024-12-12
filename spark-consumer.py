from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Kafka consumer parameters
kafka_bootstrap_servers = 'localhost:9092'
topic = 'dataset-topic'

# Cassandra parameters
cassandra_host = 'localhost'
keyspace = 'mykeyspace'
table = 'referrer_resource'

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
            print(messages)
            yield messages
            messages = []
    if messages:
        yield messages

# Function to process data with Spark
def process_with_spark(data):
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .master("local") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .getOrCreate()
    
    df = spark.createDataFrame(data, "string").toDF("line")
    
    # Example transformation: Create key-value pairs
    pairs = df.rdd.flatMap(lambda line: [
        (line.split()[0], line.split()[1]),
        (line.split()[1], line.split()[0])
    ])
    
    # Convert to DataFrame
    pairs_df = pairs.toDF(["referrer", "resource"])
    
    # Write to Cassandra
    pairs_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=table, keyspace=keyspace) \
        .save()
    
    spark.stop()

# Main execution
if __name__ == "__main__":
    # Connect to Cassandra and create keyspace and table if not exists
    cluster = Cluster([cassandra_host])
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    """)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
            referrer text PRIMARY KEY,
            resource text
        )
    """)
    
    for messages in consume_from_kafka(topic, kafka_bootstrap_servers):
        process_with_spark(messages)