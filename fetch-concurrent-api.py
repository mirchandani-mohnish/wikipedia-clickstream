import os
import requests
from bs4 import BeautifulSoup
import shutil
from kafka import KafkaProducer
import gzip
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark import SparkFiles

# Define constants
define_url = "https://dumps.wikimedia.org/other/clickstream/"
output_file = "available_dates.txt"
kafka_bootstrap_servers = 'localhost:9092'  # Kafka server address
topic = 'dataset-topic'  # Kafka topic to send the data to

def fetch_dates():
    """Fetch the list of available dates from the Wikimedia Clickstream page."""
    response = requests.get(define_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract dates from the links
    dates = [a.text.strip('/') for a in soup.find_all('a') if a.text.strip('/').startswith('20')]
    return dates

def read_local_dates():
    """Read dates from the local file."""
    if os.path.exists(output_file):
        with open(output_file, "r") as file:
            return file.read().splitlines()
    return []

def write_dates(dates):
    """Write dates to the local file."""
    with open(output_file, "w") as file:
        file.write("\n".join(dates))

def download_and_process(date, data_folder, topic):
    """Download and process a single clickstream file."""
    file_url = f"{define_url}{date}/clickstream-enwiki-{date}.tsv.gz"
    local_filename = f"{data_folder}/clickstream-enwiki-{date}.tsv.gz"
    print(f"Downloading {file_url}...")
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
        with open(local_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded {local_filename}")
        
        extracted_file = unzip_dataset(local_filename, f"{data_folder}/clickstream-enwiki-{date}.tsv")
        if extracted_file:
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
            send_to_kafka(extracted_file, producer, topic)
        
        # Close the Kafka producer
        producer.close()
    else:
        print(f"Failed to download {file_url}")

def unzip_dataset(zip_path, extract_to):
    """Unzip the downloaded file."""
    try:
        with gzip.open(zip_path, 'rb') as gz_file:
            with open(extract_to, 'wb') as out_file:
                shutil.copyfileobj(gz_file, out_file)
        print("Extraction completed successfully!")
        return extract_to
    except Exception as e:
        print(f"Error extracting file: {e}")
        return None

def send_to_kafka(file_path, producer, topic):
    """Read the unzipped file and send data to Kafka."""
    with open(file_path, 'r') as file:
        for line in file:
            message = line.strip()
            producer.send(topic, value=message.encode('utf-8'))
            print(f"Sent: {message}")

def process_with_spark(spark, dates, data_folder, topic):
    """Process the dates with Spark."""
    rdd = spark.sparkContext.parallelize(dates)
    rdd.foreach(lambda date: download_and_process(date, data_folder, topic))

def main():
    """Main function to check for updates and download new data."""
    print("Fetching available dates from the website...")
    online_dates = fetch_dates()
    print("Reading local dates...")
    local_dates = read_local_dates()
    
    data_folder = "./data"
    os.makedirs(data_folder, exist_ok=True)

    
    # Find new dates
    new_dates = sorted(set(online_dates) - set(local_dates))
    if new_dates:
        print(f"New dates found: {new_dates}")
        
        # Create Spark session
        # .master("spark://spark-master:7077") \
        spark = SparkSession.builder \
            .appName("ClickstreamProcessor") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
            .getOrCreate()
        
        # Process the dates with Spark
        process_with_spark(spark, new_dates, data_folder, topic)

        # Update the local file
        updated_dates = sorted(set(local_dates + new_dates))
        write_dates(updated_dates)
        print("Local dates file updated.")
        
    else:
        print("No new dates found.")

if __name__ == "__main__":
    main()