# ========================================================
# Python Script: Data Center Information Collector
# ========================================================
# Purpose:
# This script collects data center information from https://www.datacenters.com/locations.
# Example data includes: Name, Description, Location (Address, Latitude, Longitude), and Size.
#
# Libraries used: Selenium, BeautifulSoup, JSON, CSV
# --------------------------------------------------------

# ==========================
# 1. Import Libraries
# ==========================
import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

# ==========================
# 2. Configuration
# ==========================
DRIVER_PATH = r"~\chromedriver.exe"  # Path to ChromeDriver


# ==========================
# 3. Helper Functions
# ==========================
def setup_driver():
    """Initialize and configure the Selenium WebDriver."""
    service = Service(DRIVER_PATH)
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run Chrome in headless mode
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    return webdriver.Chrome(service=service, options=options)


def clean_text(text):
    """Clean text by removing HTML tags and non-ASCII characters."""
    clean_text = re.sub(r"<\/?[^>]+>", "", text)
    clean_text = clean_text.encode('ascii', 'ignore').decode('ascii')
    return clean_text.strip()


def format_date(date_string):
    """Convert ISO timestamp to YYYY-MM-DD format."""
    if date_string:
        try:
            return datetime.fromisoformat(date_string).strftime('%Y-%m-%d')
        except ValueError:
            return "Invalid Date"
    return "N/A"


# ==========================
# 4. Data Collection Functions
# ==========================
def get_data_center_urls(driver, page_number):
    """Fetch all data center URLs from a specific page."""
    base_url = f"https://www.datacenters.com/locations?page={page_number}"
    driver.get(base_url)

    # Handle cookie banner if present
    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#onetrust-reject-all-handler"))
        ).click()
    except Exception:
        pass  # No cookie banner found

    # Scroll to load all dynamic content
    print(f"Scrolling on page {page_number}...")
    body = driver.find_element(By.TAG_NAME, "body")
    for _ in range(3):
        body.send_keys(Keys.END)
        time.sleep(2)  # Allow time for content to load

    # Extract URLs
    tiles = driver.find_elements(By.CSS_SELECTOR, ".LocationsIndex__tiles__Sc6sW > div > a")
    urls = [tile.get_attribute("href") for tile in tiles]
    print(f"Found {len(urls)} data center URLs on page {page_number}.")
    return urls


def extract_data_from_url(url):
    """Extract detailed information from a data center webpage."""
    driver = setup_driver()
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    # Extract location data from script
    location_script = soup.select_one("body > div.page-wrapper > script:nth-child(6)")
    if not location_script:
        print(f"Location data not found for {url}")
        return None

    try:
        location_data = json.loads(location_script.string.strip())
        flattened_data = {key: value for key, value in location_data.get("location", {}).items()}
        for field in ["description", "summary", "fullAddress"]:
            if field in flattened_data:
                flattened_data[field] = clean_text(flattened_data[field])
    except Exception as e:
        print(f"Error extracting data from {url}: {e}")
        return None

    # Extract creation and update dates
    created_at, updated_at = "N/A", "N/A"
    react_script = soup.select_one("body > script.js-react-on-rails-component")
    if react_script:
        try:
            react_data = json.loads(react_script.string.strip())
            resources = react_data.get("resources", [])
            for resource in resources:
                if resource.get("id") == 1:
                    created_at = format_date(resource.get("created_at", "N/A"))
                    updated_at = format_date(resource.get("updated_at", "N/A"))
        except Exception as e:
            print(f"Error extracting created_at/updated_at: {e}")

    flattened_data["created_at"] = created_at
    flattened_data["updated_at"] = updated_at

    return flattened_data


# ==========================
# 5. Output Functions
# ==========================
def save_to_csv(data, filename):
    """Save data to a CSV file."""
    fixed_order = ["id", "name", "description", "summary"]
    dynamic_fields = set()
    for entry in data:
        dynamic_fields.update(entry.keys())

    dynamic_fields = list(dynamic_fields - set(fixed_order))
    fieldnames = fixed_order + dynamic_fields

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Data saved to {filename}")


# ==========================
# 6. Main Function
# ==========================
def main():
    """Main script for data collection."""
    driver = setup_driver()
    all_data = []

    # Define page ranges to scrape
    page_ranges = [(1, 50), (51, 100), (101, 105)]

    for start, end in page_ranges:
        data = []
        for page_number in range(start, end + 1):
            print(f"Processing page {page_number}...")
            urls = get_data_center_urls(driver, page_number)

            for url in urls:
                print(f"Processing {url}")
                result = extract_data_from_url(url)
                if result:
                    data.append(result)

        # Save intermediate results
        filename = f"~\\data_page_{start}_to_{end}.csv"
        save_to_csv(data, filename)
        all_data.extend(data)

    driver.quit()

    # Save final merged data
    save_to_csv(all_data, r"~\final.csv")

# ==========================
# 7. Script Execution
# ==========================
if __name__ == "__main__":
    main()
