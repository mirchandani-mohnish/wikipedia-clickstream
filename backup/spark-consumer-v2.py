from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql.functions import split, col, regexp_replace, lower, sum, when,to_timestamp
from pyspark.sql.types import IntegerType


# Kafka consumer parameters
kafka_bootstrap_servers = 'localhost:9092'
topic = 'dataset-topic'

# Cassandra parameters
cassandra_host = 'localhost'
keyspace = 'mykeyspace'
table1 = 'resource_destination'
table2 = 'trending_topics'
    
    
def streamAndRun(topic, kafka_bootstrap_servers):     
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .master("local") \
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

    print(df)

    # Split the value column based on the delimiter and create key-value pairs
    delimiter = '\t'  # Define your delimiter here
    pairs = df.withColumn("referrer", split(col("value"), delimiter).getItem(0)) \
            .withColumn("resource", split(col("value"), delimiter).getItem(1)) \
            .select("referrer", "resource")
    
    # Debugging: Print the formatted data
    print_query = pairs.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Write the results to Cassandra
    query = pairs.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table1) \
        .option("checkpointLocation", "/tmp/spark-checkpoints") \
        .outputMode("append") \
        .start()

    print_query.awaitTermination()  # Print formatted data
    # Wait for the termination of the query
    query.awaitTermination()

def preprocess_and_write(topic, kafka_bootstrap_servers):
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .master("local") \
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

   # Split the value column into source, search_term, type, and count
    delimiter = '\t'  # Define your delimiter
    data = df.withColumn("source", split(col("value"), delimiter).getItem(0)) \
             .withColumn("search_term", split(col("value"), delimiter).getItem(1)) \
             .withColumn("type", split(col("value"), delimiter).getItem(2)) \
             .withColumn("count", split(col("value"), delimiter).getItem(3).cast(IntegerType()))\
             .select("search_term", "source", "type", "count")

    # Define the condition for rows to be normalized
    # List of values to exclude from normalization
    exclude_list = ['other-internal', 'other-search', 'other-external', 'other-empty', 'other-other']

    condition_source = ~col("source").isin(exclude_list)

    # Apply conditional normalization (replace special characters with spaces)
    data_normalized = data \
        .withColumn("source", when(condition_source, 
                                 lower(regexp_replace(col("source"), r'[^a-zA-Z0-9]', ' '))) \
                            .otherwise(col("source"))) \
        .withColumn("search_term", lower(regexp_replace(col("search_term"), r'[^a-zA-Z0-9 ]', ' ')))

    # For Trending Topics Table
    # Add a dummy timestamp column if it doesn't exist
    # data_with_timestamp = data_normalized.withColumn("timestamp", to_timestamp(col("count")))  # Replace with real timestamp if available

    # Add watermark
    # trending_data = data_with_timestamp \
    #     .withWatermark("timestamp", "10 minutes") \
    #     .groupBy("search_term") \
    #     .agg(sum("count").alias("traffic_count"))

    # Write Trending Topics Data to Cassandra
    # trending_query = trending_data.writeStream \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .option("keyspace", keyspace) \
    #     .option("table", table2) \
    #     .outputMode("append") \
    #     .option("checkpointLocation", "/tmp/trending-checkpoints") \
    #     .start()

    # Write Source-Destination Data to Cassandra
    source_dest_query = data_normalized.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", keyspace) \
        .option("table", table1) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/source-dest-checkpoints") \
        .start()

    # Wait for termination
    # trending_query.awaitTermination()
    source_dest_query.awaitTermination()


# Main execution
if __name__ == "__main__":
    # Connect to Cassandra and create keyspace and table if not exists
    cluster = Cluster([cassandra_host])
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}
    """)
    # session.execute(f"""
    #     CREATE TABLE IF NOT EXISTS {keyspace}.{table1} (
    #         referrer text PRIMARY KEY,
    #         resource text
    #     )
    # """)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table1} (
            search_term TEXT,
            source TEXT,
            type TEXT,
            count INT,
            PRIMARY KEY (search_term, source, type)
        )
    """)

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table2} (
            search_term TEXT PRIMARY KEY,
            traffic_count INT
        )
    """)
    
    preprocess_and_write(topic, kafka_bootstrap_servers)
    
    # streamAndRun(topic, kafka_bootstrap_servers)
    # for messages in consume_from_kafka(topic, kafka_bootstrap_servers):
    #     process_with_spark(messages)


# Function to consume data from Kafka
# def consume_from_kafka(topic, bootstrap_servers):
#     consumer = KafkaConsumer(
#         topic,
#         bootstrap_servers=bootstrap_servers,
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         group_id='my-group',
#         value_deserializer=lambda x: x.decode('utf-8')
#     )
#     messages = []
#     for message in consumer:
#         messages.append(message.value)
#         print(message.value.split())
#         if len(messages) >= 1000:  # Process in batches of 1000 messages
           
#             yield messages
#             messages = []
#     if messages:
#         yield messages

# # Function to process data with Spark
# def process_with_spark(data):
#     spark = SparkSession.builder \
#         .appName("KafkaSparkConsumer") \
#         .master("local") \
#         .config("spark.cassandra.connection.host", cassandra_host) \
#         .getOrCreate()
    
#     df = spark.createDataFrame(data, "string").toDF("line")
    
#     # Example transformation: Create key-value pairs
#     pairs = df.rdd.flatMap(lambda line: [
#         (line.split('\t')[0], line.split('\t')[1]),
#         (line.split('\t')[1], line.split('\t')[0])
#     ])
    
#     # Convert to DataFrame
#     pairs_df = pairs.toDF(["referrer", "resource"])
    
#     # Write to Cassandra
#     pairs_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .mode("append") \
#         .options(table=table, keyspace=keyspace) \
#         .save()
    
#     spark.stop()