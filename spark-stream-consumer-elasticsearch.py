from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Kafka consumer parameters
kafka_bootstrap_servers = 'localhost:9092'
topic = 'dataset-topic'

# Elasticsearch parameters
es_host = 'http://localhost:9201'
es_index = 'referrer_resource'

def streamAndRun(topic, kafka_bootstrap_servers):     
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .master("local[*]") \
        .config("spark.es.nodes", es_host) \
        .config("spark.es.port", "9201") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.9.0") \
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

    # Write the results to Elasticsearch
    query = pairs.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", es_index) \
        .option("es.nodes", es_host) \
        .option("checkpointLocation", "/tmp/spark-checkpoints") \
        .outputMode("append") \
        .start()

    # Wait for the termination of the query
    query.awaitTermination()

# Main execution
if __name__ == "__main__":
    streamAndRun(topic, kafka_bootstrap_servers)