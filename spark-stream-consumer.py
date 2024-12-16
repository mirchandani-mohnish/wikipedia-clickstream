from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark.sql.functions import split, col, regexp_replace, lower, sum, when,to_timestamp
import re


# Kafka consumer parameters
kafka_bootstrap_servers = 'localhost:9092'
topic = 'dataset-topic'

# Cassandra parameters
cassandra_host = 'localhost'
keyspace = 'mykeyspace'
table = 'referrer_resource'
search_table = 'search_index'

# Function to tokenize a string
def tokenize(value):
    stop_words = [
    'a', 'an', 'the', 'and', 'or', 'but', 'is', 'are', 'was', 'were', 
    'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
    'not', 'no', 'of', 'off', 'in', 'on', 'at', 'by', 'with', 'about', 
    'into', 'to', 'from', 'up',  'out', 'over', 'under',
    'I', 'you', 'he', 'she', 'it', 'we', 'they', 'me', 'my', 'mine', 
    'our', 'ours', 'your', 'yours', 'his', 'her', 'its', 'their', 'theirs'
    ]

    # Combine this with your domain-specific exclude list
    exclude_list = ['other-internal', 'other-search', 'other-external', 'other-empty', 'other-other']
    full_exclude_list = set(stop_words + exclude_list)
    # return [word.lower() for word in regexp_replace(value, r'[^a-zA-Z0-9 ]', ' ').split() if word]

    # Normalize and split into tokens
    # tokens = [word.lower() for word in regexp_replace(value, r'[^a-zA-Z0-9 ]', ' ').split() if word]

     # Replace non-alphanumeric characters with spaces using re.sub
    value_cleaned = re.sub(r'[^a-zA-Z0-9 ]', ' ', value)
    tokens = [word.lower() for word in value_cleaned.split() if word and word not in full_exclude_list]


    # Filter out excluded tokens
    return [token for token in tokens if token not in full_exclude_list]


# Function to insert tokens into the search_index table
def insert_into_search_table(session, resource, term):

    tokens = tokenize(term)
    insert_query = SimpleStatement(f"""
        INSERT INTO {search_table} (word, resource) VALUES (%s, %s) IF NOT EXISTS
    """)
    for token in set(tokens):  # Use a set to avoid duplicate tokens
        session.execute(insert_query, (token, resource))

def upsert_resource( session, referrer, resource, type, count):
    
    # Upsert the resource pair
    update_query = SimpleStatement(f"""
        UPDATE {table}
        SET count = count + %s
        WHERE referrer = %s AND resource = %s
        IF EXISTS;
    """)
    # Attempt to update; if not successful, insert instead
    result = session.execute(update_query, (count, referrer, resource))
    if not result[0].applied:
        # Insert if the row does not exist
        insert_query = SimpleStatement(f"""
            INSERT INTO {table} (referrer, resource,type, count)
            VALUES (%s, %s, %s, %s)
            IF NOT EXISTS;
        """)
        session.execute(insert_query, (referrer, resource, type, count))

        # Tokenize and insert referrer and resource into search_index
    # insert_tokens(session, resource, referrer)
    insert_into_search_table(session, resource, resource)

    


def process_batch(session, batch_df, batch_id):
    records = batch_df.collect()
    ctr = 0
    
    for record in records:
        ctr += 1
        if(ctr % 1000 == 0):
            print(f"Processed {ctr} records")
        upsert_resource(session, record['referrer'], record['resource'], record['type'], record['count'])
    


def streamAndRun(session, topic, kafka_bootstrap_servers):     
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
        .option("failOnDataLoss", "false") \
        .option("subscribe", topic) \
        .load()

    # Convert the value column to string
    df = df.selectExpr("CAST(value AS STRING)")
    
    

    # Split the value column based on the delimiter and create key-value pairs
    delimiter = '\t'  # Define your delimiter here
    pairs = df.withColumn("referrer", split(col("value"), delimiter).getItem(0)) \
            .withColumn("resource", split(col("value"), delimiter).getItem(1)) \
            .withColumn("type", split(col("value"), delimiter).getItem(2)) \
            .withColumn("count", split(col("value"), delimiter).getItem(3).cast("bigint")) \
            .select("referrer", "resource", "type", "count")

    # Write the results to Cassandra
    # query = pairs.writeStream \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .option("keyspace", keyspace) \
    #     .option("table", table) \
    #     .option("checkpointLocation", "/tmp/spark-checkpoints") \
    #     .outputMode("append") \
    #     .start()
    
     # List of values to exclude from normalization
    exclude_list = ['other-internal', 'other-search', 'other-external', 'other-empty', 'other-other']

    condition_source = ~col("referrer").isin(exclude_list)

    # Apply conditional normalization (replace special characters with spaces)
    data_normalized = pairs \
        .withColumn("referrer", when(condition_source, 
                                 lower(regexp_replace(col("referrer"), r'[^a-zA-Z0-9]', ' '))) \
                            .otherwise(col("referrer"))) \
        .withColumn("resource", lower(regexp_replace(col("resource"), r'[^a-zA-Z0-9 ]', ' ')))

    # Repartition the DataFrame to increase parallelism
    # data_normalized = data_normalized.repartition(10)


    # query = data_normalized.writeStream \
    #     .foreachBatch(process_batch) \
    #     .option("checkpointLocation", "/tmp/spark-checkpoints") \
    #     .start()
    
    query = data_normalized.writeStream \
        .foreachBatch(lambda batch_df, batch_id: process_batch(session, batch_df, batch_id)) \
        .option("checkpointLocation", "/tmp/spark-checkpoints") \
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
            referrer text,
            resource text,
            type text,
            count BIGINT,
            PRIMARY KEY (referrer, resource)
        )
    """)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS mykeyspace.search_index (
            word TEXT,
            resource TEXT,
            PRIMARY KEY (word, resource)
                )
    """)

    session = cluster.connect(keyspace)
    
    streamAndRun(session, topic, kafka_bootstrap_servers)
    session.shutdown()
    cluster.shutdown()
