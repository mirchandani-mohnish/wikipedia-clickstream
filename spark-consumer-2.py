from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql.functions import split, col, regexp_replace, lower, sum, when, current_timestamp
from pyspark.sql.types import IntegerType


# Kafka consumer parameters
kafka_bootstrap_servers = 'localhost:9092'
topic = 'dataset-topic'

# Cassandra parameters
cassandra_host = 'localhost'
keyspace = 'mykeyspace'
table1 = 'resource_destination'

es_nodes = 'http://localhost:9200'
# es_nodes = 'http://elasticsearch:9200'


def stream_preprocess_and_write(topic, kafka_bootstrap_servers):
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .master("local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2")\
        .config("es.nodes", es_nodes) \
        .getOrCreate()


    # Read data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .load()


    # Convert the value column to string
    df = df.selectExpr("CAST(value AS STRING)")

   # Split the value column into source, search_term, type, and count
    delimiter = '\t'  # Define your delimiter
    data = df.withColumn("source", split(col("value"), delimiter).getItem(0)) \
             .withColumn("search_term", split(col("value"), delimiter).getItem(1)) \
             .withColumn("type", split(col("value"), delimiter).getItem(2)) \
             .withColumn("count", split(col("value"), delimiter).getItem(3).cast(IntegerType()))\
             .withColumn("timestamp", current_timestamp())\
             .select("search_term", "source", "type", "count", "timestamp")

     # Normalize data
    # List of values to exclude from normalization
    exclude_list = ['other-internal', 'other-search', 'other-external', 'other-empty', 'other-other']
    condition_source = ~col("source").isin(exclude_list)

    # Apply conditional normalization (replace special characters with spaces)
    data_normalized = data \
        .withColumn("source", when(condition_source, 
                                 lower(regexp_replace(col("source"), r'[^a-zA-Z0-9]', ' '))) \
                            .otherwise(col("source"))) \
        .withColumn("search_term", lower(regexp_replace(col("search_term"), r'[^a-zA-Z0-9 ]', ' ')))

    # Write data to Elasticsearch
    query = data_normalized.writeStream \
        .format("org.elasticsearch.spark.sql")\
        .option("es.resource", f"{table1}/_doc")\
        .option("es.nodes", es_nodes) \
        .option("checkpointLocation", "/tmp/es-checkpoints") \
        .outputMode("append") \
        .start()


    # Wait for termination
    query.awaitTermination()


# Main execution
if __name__ == "__main__":
    stream_preprocess_and_write(topic, kafka_bootstrap_servers)
