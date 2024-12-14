from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql.functions import split, col


# Kafka consumer parameters
kafka_bootstrap_servers = 'localhost:9092'
topic = 'dataset-topic'

# Cassandra parameters
cassandra_host = 'localhost'
keyspace = 'mykeyspace'
table = 'referrer_resource'

def streamAndRun(topic, kafka_bootstrap_servers):     
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .master("local[*]") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .getOrCreate()

    # Read data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .load()

    # Convert the value column to string
    df = df.selectExpr("CAST(value AS STRING)")

    # Split the value column based on the delimiter and create key-value pairs
    delimiter = '\t'  # Define your delimiter here
    pairs = df.withColumn("referrer", split(col("value"), delimiter).getItem(0)) \
            .withColumn("resource", split(col("value"), delimiter).getItem(1)) \
            .select("referrer", "resource")

    # Write the results to Cassandra
    query = pairs.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table) \
        .option("checkpointLocation", "/tmp/spark-checkpoints") \
        .outputMode("append") \
        .start()

    # Wait for the termination of the query
    query.awaitTermination()

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
    
    streamAndRun(topic, kafka_bootstrap_servers)
