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



def upsert_resource(referrer, resource, type, count):
    print(count);
    cluster = Cluster([cassandra_host])
    session = cluster.connect(keyspace)
    
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
    
    session.shutdown()
    cluster.shutdown()


def process_batch(batch_df, batch_id):
    records = batch_df.collect()
    for record in records:
        upsert_resource(record['referrer'], record['resource'], record['type'], record['count'])


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
    query = pairs.writeStream \
        .foreachBatch(process_batch) \
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
    
    streamAndRun(topic, kafka_bootstrap_servers)
