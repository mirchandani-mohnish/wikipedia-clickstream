import requests
from bs4 import BeautifulSoup
import datetime

# Function to fetch the HTML and extract dates
def fetch_dates(url):
    try:
        print("making request")
        response = requests.get(url)
        response.raise_for_status()
        
        print("parsing data")
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all <a> tags in the <pre> section
        pre_section = soup.find('pre')
        if not pre_section:
            raise ValueError("No <pre> section found in the HTML.")
        
        links = pre_section.find_all('a')

        # Extract the dates (assuming format YYYY-MM)
        dates = [link.text.strip('/') for link in links if link.text.strip('/').startswith(('19', '20'))]
        
        return dates
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# URL to fetch
def main():
    url = "https://dumps.wikimedia.org/other/clickstream"  # Replace with the actual API URL
    dates = fetch_dates(url)
    
    if dates:
        print("Extracted dates:", dates)
        
        # Save dates to a file with a timestamp
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        with open(f"dates_{today}.txt", "w") as f:
            f.write("\n".join(dates))
        print(f"Dates saved to dates_{today}.txt")
    else:
        print("No dates were found.")

if __name__ == "__main__":
    main()
