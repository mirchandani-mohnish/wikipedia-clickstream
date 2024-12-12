import os
import requests
from bs4 import BeautifulSoup

# Define constants
define_url = "https://dumps.wikimedia.org/other/clickstream/"
output_file = "available_dates.txt"

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

def download_clickstream_data(new_dates):
    """Download clickstream gzip files for the new dates."""
    for date in new_dates:
        file_url = f"{define_url}{date}/clickstream-enwiki-{date}.tsv.gz"
        local_filename = f"clickstream-enwiki-{date}.tsv.gz"
        print(f"Downloading {file_url}...")
        response = requests.get(file_url, stream=True)
        if response.status_code == 200:
            with open(local_filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded {local_filename}")
        else:
            print(f"Failed to download {file_url}")

def main():
    """Main function to check for updates and download new data."""
    print("Fetching available dates from the website...")
    online_dates = fetch_dates()
    print("Reading local dates...")
    local_dates = read_local_dates()

    # Find new dates
    new_dates = sorted(set(online_dates) - set(local_dates))
    if new_dates:
        print(f"New dates found: {new_dates}")
        download_clickstream_data(new_dates)

        # Update the local file
        updated_dates = sorted(set(local_dates + new_dates))
        write_dates(updated_dates)
        print("Local dates file updated.")
    else:
        print("No new dates found.")

if __name__ == "__main__":
    main()