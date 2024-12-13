import os
import requests
from bs4 import BeautifulSoup
import shutil
from kafka import KafkaProducer
import gzip
import time

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

def download_clickstream_data(data_folder,new_dates):
    """Download clickstream gzip files for the new dates."""
    for date in new_dates:
        file_url = f"{define_url}{date}/clickstream-enwiki-{date}.tsv.gz"
        local_filename = f"{data_folder}/clickstream-enwiki-{date}.tsv.gz"
        print(f"Downloading {file_url}...")
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            with open(local_filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded {local_filename}")
        
            return unzip_dataset(local_filename, f"{data_folder}/clickstream-enwiki-{date}.tsv")
        else:
            print(f"Failed to download {file_url}")
            return None


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
        if filename.endswith('.tsv'):  # Assuming CSV file format
            file_path = os.path.join(extracted_folder, filename)
            with open(file_path, 'r') as file:
                for line in file:
                    message = line.strip()
                    producer.send(topic, value=message.encode('utf-8'))
                    print(f"Sent: {message}")
                    time.sleep(0.1)  # To prevent overwhelming Kafka
                    

def main():
    """Main function to check for updates and download new data."""
    print("Fetching available dates from the website...")
    # # online_dates = fetch_dates()
    # print("Reading local dates...")
    # local_dates = read_local_dates()
    
    data_folder = "./data"

    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    
    # Find new dates
    
    # Step 4: Read the unzipped file and send to Kafka
    send_to_kafka(data_folder, producer, topic)

    producer.close()


if __name__ == "__main__":
    main()