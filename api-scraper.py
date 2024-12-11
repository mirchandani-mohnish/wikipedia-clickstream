import requests
import zipfile
import os
from kafka import KafkaProducer
import time
import shutil
import gzip 


# URL to download the dataset
dataset_url = "https://qrank.wmcloud.org/download/qrank.csv.gz"  # Replace with the actual dataset URL

# Define Kafka producer parameters
kafka_bootstrap_servers = 'localhost:9092'  # Kafka server address
topic = 'dataset-topic'  # Kafka topic to send the data to

# Function to download the dataset from the URL
def download_dataset(url, download_path):
    print(f"Downloading dataset from {url}...")
    response = requests.get(url)
    with open(download_path, 'wb') as file:
        file.write(response.content)
    print("Download completed!")

# Function to unzip the downloaded file
def unzip_dataset(zip_path, extract_to):
    # print(f"Unzipping dataset to {extract_to}...")
    # with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    #     zip_ref.extractall(extract_to)
    # print("Unzip completed!")
    try:
        with gzip.open(zip_path, 'rb') as gz_file:
            with open(extract_to, 'wb') as out_file:
                shutil.copyfileobj(gz_file, out_file)
        print("Extraction completed successfully!")
        return extract_to
    except Exception as e:
        print(f"Error extracting file: {e}")
        return None

# Function to read the unzipped file and send data to Kafka
def send_to_kafka(extracted_folder, producer, topic):
    # Assuming the unzipped file is CSV and we process it line by line
    for filename in os.listdir(extracted_folder):
        if filename.endswith('.csv'):  # Assuming CSV file format
            file_path = os.path.join(extracted_folder, filename)
            with open(file_path, 'r') as file:
                for line in file:
                    message = line.strip()
                    producer.send(topic, value=message.encode('utf-8'))
                    print(f"Sent: {message}")
                    # time.sleep(0.1)  # To prevent overwhelming Kafka

# Main execution
if __name__ == "__main__":
    download_path = "dataset.gz"  # Path to save the downloaded zip file
    extracted_folder = "extracted_data"  # Folder to extract the contents of the zip file

    # Step 1: Download the dataset
    download_dataset(dataset_url, download_path)

    # Step 2: Unzip the dataset
    if not os.path.exists(extracted_folder):
        os.makedirs(extracted_folder)
    unzip_dataset(download_path, os.path.join(extracted_folder, "qrank.csv"))

    # Step 3: Set up Kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    # Step 4: Read the unzipped file and send to Kafka
    send_to_kafka(extracted_folder, producer, topic)

    # Close the Kafka producer
    producer.close()
