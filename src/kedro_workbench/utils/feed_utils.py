import os
import feedparser
import pprint
import yaml
from datetime import datetime
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (TimeoutException, StaleElementReferenceException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import inspect
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from kedro_workbench.utils.feature_engineering import get_day_of_week
import uuid
import hashlib
from datetime import datetime
from dateutil import parser
from icecream import ic
import pandas as pd
ic.configureOutput(includeContext=True)
from selenium.common.exceptions import WebDriverException
chrome_options = Options()
chrome_options.add_argument("--headless")

import logging
logger = logging.getLogger(__name__)

def generate_feed_structure_text(feed_url, feed_name, output_dir=None):
    parsed_feed = feedparser.parse(feed_url)
    pp = pprint.PrettyPrinter(indent=4)

    if output_dir is None:
        output_dir = os.path.join("data", "01_raw")

    # Create a subdirectory based on the feed name
    feed_output_dir = os.path.join(output_dir, feed_name)
    os.makedirs(feed_output_dir, exist_ok=True)

    # Append the UTC time to the file name
    utc_time = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    file_name = f"feed_structure_{utc_time}.txt"
    file_path = os.path.join(feed_output_dir, file_name)

    schema = {}
    for key, value in parsed_feed.entries[0].items():
        if isinstance(value, dict):
            # If the value is a nested dictionary, get its keys
            schema[key] = list(value.keys())
        else:
            schema[key] = None

    with open(file_path, "w", encoding="utf-8") as file:
        file.write(f"BOZO Score: {parsed_feed.bozo}\n")
        pp_str = pp.pformat(schema)
        print(pp_str, file=file)  # Save the pretty-printed output to the file


def generate_feed_structure_yaml(feed_url, feed_name, output_dir=None):
    parsed_feed = feedparser.parse(feed_url)
    pp = pprint.PrettyPrinter(indent=4)
    pretty_structure = pp.pformat(parsed_feed)

    if output_dir is None:
        output_dir = os.path.join("data", "01_raw")

    # Create a subdirectory based on the feed name
    feed_output_dir = os.path.join(output_dir, feed_name)
    os.makedirs(feed_output_dir, exist_ok=True)

    # Append the UTC time to the file name
    utc_time = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    file_name = f"feed_structure_{utc_time}.yml"
    file_path = os.path.join(feed_output_dir, file_name)

    with open(file_path, "w", encoding="utf-8") as file:
        file.write(pretty_structure)


def replace_multiple_newlines(text):
    pattern = r"(\s*\n\s*){2,}"  # Matches 2 or more consecutive newline characters with optional spaces around
    replacement = " \n"  # Replace with a single newline character

    cleaned_text = re.sub(pattern, replacement, text)
    return cleaned_text


def extract_id_from_url(url):
    # Match the GUID pattern at the end of the URL
    guid_pattern = r"[-\w]{8}-[-\w]{4}-[-\w]{4}-[-\w]{4}-[-\w]{12}$"
    guid_match = re.search(guid_pattern, url)

    # Match the "kb" code pattern in the URL
    kb_pattern = r"kb\d{6,}"
    kb_match = re.search(kb_pattern, url)

    if kb_match:
        return kb_match.group()
    elif guid_match:
        return guid_match.group()
    else:
        url_parts = url.split("/")
        if url_parts:
            return url_parts[-1]
        else:
            return None


def is_valid_date_format(date_string):
    # Define a regex pattern for "dd-mm-YYYY" format
    date_pattern = r"\d{2}-\d{2}-\d{4}"

    # Use re.match to check if the date string matches the pattern
    if re.match(date_pattern, date_string):
        return True
    else:
        return False


def is_partial_date(input_date):
    # Define a regular expression pattern for a partial date
    pattern = r"^(?:january|february|march|april|may|june|july|august|september|october|november|december)-\d{1,2}$"

    # Check if the input_date matches the pattern (case-insensitive)
    return bool(re.match(pattern, input_date, re.IGNORECASE))

def complete_partial_date(partial_date, year):
    if not is_partial_date(partial_date):
        raise ValueError("Invalid partial date format")

    # Mapping of month names to numbers
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12
    }

    # Split the partial date into month and day
    month_str, day_str = partial_date.lower().split('-')
    month = month_map[month_str]
    day = int(day_str)

    # Get the current year
    current_year = year

    # Create and return the complete date string
    return f"{day:02d}-{month:02d}-{current_year}"

def remove_day_suffix(date_input):
    # Define a regular expression pattern to match dates with optional suffix
    # print(f"remove_day_suffix: {date_input}")
    pattern = r"(\w+)-(\d{1,2})(?:st|nd|rd|th)?-(\d{4})"

    # Check if the pattern is matched
    if re.match(pattern, date_input, flags=re.IGNORECASE):
        # Use re.sub to remove the suffix
        date_input = re.sub(pattern, r"\1-\2-\3", date_input, flags=re.IGNORECASE)
        # ic(date_input)
    return date_input


def convert_date_string(input_date, year):
    if is_partial_date(input_date):
        fixed_date = complete_partial_date(input_date, year)
        # print(f"partial date found is fixed -> {fixed_date}")
        return fixed_date
    if is_valid_date_format(input_date):
        # print("valid date found")
        return input_date

    input_date = remove_day_suffix(input_date)
    # print(f"attempted to remove suffix: {input_date}")
    try:
        # ic(datetime.strptime(input_date, "%B-%d-%Y"))
        date_string = input_date.capitalize()
        parsed_date = datetime.strptime(date_string, "%B-%d-%Y")
        formatted_date = parsed_date.strftime("%d-%m-%Y")
    except ValueError:
        # print("first date pattern failed")
        try:
            # Attempt to parse the "Sun, 03 Sep 2023 07:00:00 Z" format
            parsed_date = parser.parse(input_date)
            formatted_date = parsed_date.strftime("%d-%m-%Y")
        except ValueError:
            # Use the current date as a fallback
            # ic("couldnt parse date. sending back original")
            formatted_date = input_date

    return formatted_date


def generate_custom_uuid(url=None, version=None, date_str=None, collection=None):
    # Concatenate the input data into a single string
    input_str = f"{url}{version}{date_str}{collection}"
    # Hash the input string using SHA-1
    hash_obj = hashlib.sha1(input_str.encode())
    # Get the hash digest as a byte string
    hash_bytes = hash_obj.digest()
    # Format the byte string as a UUID-like string
    uuid_str = "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}".format(
        *hash_bytes
    )
    return uuid_str


def add_id_key(data):
    year = datetime.now().year
    for entry in data:
        try:
            url = entry["source"]
        except KeyError:
            url = entry["link"]
        if entry.get("revision"):
            revision = entry["revision"]
        else:
            revision = None
        published = convert_date_string(entry["published"], year)
        if not is_valid_date_format(published):
            print(f"invalid date format: {published}")
        id = generate_custom_uuid(url, revision, published, entry["collection"])
        if "id" not in entry:
            entry["id"] = id
    return data


def extract_link_content(data, params, chrome_options, collection_name=None):
    
    for i, entry in enumerate(data):
        try:
            link = entry["source"]
        except KeyError:
            link = entry["link"]
        # extract post_id from URL
        if entry.get("post_id") is None:
            entry["post_id"] = extract_id_from_url(link)
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            driver.get(link)
            time.sleep(2.1)

            # Get the page source and parse it with BeautifulSoup
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")

            filtered_content = []

            for element, attribute, value in zip(
                params["elements"], params["attributes"], params["values"]
            ):
                if attribute.strip() and value.strip():
                    if attribute == "class":
                        tags = soup.find_all(element, class_=value)
                    else:
                        tags = soup.find_all(element, attrs={attribute: value})
                else:
                    tags = soup.find_all(element)

                for tag in tags:
                    filtered_content.append(str(tag))

            page_content = " ".join(filtered_content)
            # entry["page_content"] = replace_multiple_newlines(post_content)
            entry["page_content"] = page_content
            # Extract URLs using regex
            post_soup = BeautifulSoup(entry["page_content"], "html.parser")

            for idx, a_tag in enumerate(post_soup.find_all("a", href=True), start=1):
                href = a_tag["href"]
                display_text = a_tag.get_text()
                no_text_counter = 1
                if display_text == "":
                    display_text = f"display_text_{no_text_counter}"
                    no_text_counter += 1
                if re.match(
                    r"https://cve\.mitre\.org.*", href
                ):  # Using regex to match cve.mitre.org URLs
                    entry[f"cve_link:{display_text}"] = href
                elif (
                    "msrc.microsoft.com/update-guide/vulnerability" in href
                ):  # Check for the msrc link pattern
                    entry[f"msrc_link:{display_text}"] = href

            # entry["day_of_week"] = get_day_of_week(entry["published"])
            if collection_name:
                entry["collection"] = collection_name
        finally:
            driver.close()
            time.sleep(3)
            driver.quit()
    return data


def extract_data_name():
    caller_frame = (
        inspect.currentframe().f_back
    )  # Get the frame of the calling function
    caller_name = caller_frame.f_code.co_name  # Get the name of the calling function
    match = re.match(r"augment_(\w+)_data", caller_name)

    if match:
        data_name = match.group(1)  # Extract the part within the parentheses
        # print(f"Data name: {data_name}")
    return data_name


def parse_link_content(data):
    for item in data:
        page_source = item["page_content"]
        soup = BeautifulSoup(page_source, "html.parser")

        no_text_counter = 1

        for idx, a_tag in enumerate(soup.find_all("a", href=True), start=1):
            href = a_tag["href"]
            display_text = a_tag.get_text()

            if display_text == "":
                display_text = f"display_text_{no_text_counter}"
                no_text_counter += 1
            # link_info = {display_text: href}

            if re.match(r"https://cve\.mitre\.org.*", href):
                item[f"cve_link:{display_text}"] = href
                # link_data["cve_links"].append(link_info)
            elif re.match(
                r"https://learn\.microsoft\.com/en-us/deployedge/microsoft-edge-relnotes-security.*",
                href,
            ):
                match = re.search(r"#(\w+-\d+-\d+)", href)
                if match:
                    item[f"security_link:{display_text}"] = href
                    # link_data["security_links"].append(link_info)
            elif "msrc.microsoft.com/update-guide/vulnerability" in href:
                item[f"security_link:{display_text}"] = href
                # link_data["cve_links"].append(link_info)
            else:
                if item["source"] != href:
                    item[f"content_link:{display_text}"] = href
                    # link_data["content_links"].append(link_info)
        # item['link_data']=link_data
        # item["day_of_week"] = get_day_of_week(item["published"])

    return data


def compare_dates(date_str1, date_str2):
    try:
        # Parse the first date string into a datetime object
        dt1 = datetime.strptime(date_str1, "%B-%d-%Y")
        # Convert the datetime object back to a standardized string format
        standardized_str1 = dt1.strftime("%Y-%m-%d")

        # Parse the second date string into a datetime object
        try:
            dt2 = datetime.strptime(date_str2, "%B %d, %Y")
        except ValueError:
            # Handle the anomaly by extracting month and day using regex
            match = re.match(r"(\w+)\s+(\d+)\s+-\s+(\d{4})", date_str2)
            if match:
                month, day, year = match.groups()
                dt2 = datetime(
                    int(year), datetime.strptime(month, "%B").month, int(day)
                )

        # Convert the datetime object back to a standardized string format
        standardized_str2 = dt2.strftime("%Y-%m-%d")

        # Compare the standardized strings for equality
        return standardized_str1 == standardized_str2
    except ValueError:
        # Handle parsing errors
        return False


def get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=False):
    new_data = []
    try:
        with MongoClient(mongo_url) as client:
            db = client[mongo_db]
            collection = db[mongo_collection]
            ic(f"num docs ({len(data)}): db:{mongo_db} collection:{mongo_collection}")
            if not nested_id:
                print("not nested")
                cursor = collection.find({}, {"_id": 0, "id": 1})
                existing_ids = [doc["id"] for doc in cursor]
                ic(f"found {len(existing_ids)} ids")
                new_data = [item for item in data if item["id"] not in existing_ids]
                """ if all("revision" in item for item in data):
                    print("revision detected")
                    cursor = collection.find({}, {"_id": 0, "id": 1, "revision": 1})
                    existing_docs = [(doc["id"], doc["revision"]) for doc in cursor]      
                    print(f"found {len(existing_docs)} ids")
                    data_to_update = []
                    for idx, item in enumerate(data):
                        document_id = item["id"]
                        document_revision = item["revision"]
                        search_tuple = (document_id, document_revision)
                        
                        if search_tuple not in existing_docs:
                            #print(f"new doc: {search_tuple}")
                            data_to_update.append(idx)
                    new_data = [data[i] for i in data_to_update]
                else:
                    print("no revision")
                    cursor = collection.find({}, {"_id": 0, "id": 1})
                    existing_ids = [doc["id"] for doc in cursor]
                    print(f"found {len(existing_ids)} ids")
                    new_data = [item for item in data if item["id"] not in existing_ids] """
            else:
                print("nested")
                cursor = collection.find({}, {"_id": 0, "metadata.id": 1})
                existing_ids = [doc["metadata"]["id"] for doc in cursor]
                ic(f"found {len(existing_ids)} ids")
                new_data = [item for item in data if item["id"] not in existing_ids]
                """ if all("revision" in item for item in data):
                    print("revision detected")
                    cursor = collection.find({}, {"_id": 0, "metadata.id": 1, "metadata.revision": 1})
                    existing_docs = [(doc["metadata"]["id"], doc["metadata"]["revision"]) for doc in cursor]      
                    print(f"found {len(existing_docs)} ids")
                    data_to_update = []
                    for idx, item in enumerate(data):
                        document_id = item["id"]
                        document_revision = item["revision"]
                        search_tuple = (document_id, document_revision)
                        
                        if search_tuple not in existing_docs:
                            #print(f"new doc: {search_tuple}")
                            data_to_update.append(idx)
                    new_data = [data[i] for i in data_to_update]
                else:
                    print("no revision")
                    cursor = collection.find({}, {"_id": 0, "metadata.id": 1})
                    existing_ids = [doc["metadata"]["id"] for doc in cursor]
                    print(f"found {len(existing_ids)} ids")
                    new_data = [item for item in data if item["id"] not in existing_ids] """
            print(f"num new data: {len(new_data)}")
    except PyMongoError as error:
        print(str(error))

    return new_data

def wait_for_specific_file(download_path, expected_extension=".xlsx", timeout=120):
    """
    Waits for a file with the specified extension to appear in the download directory.
    """
    sleep_interval = 2  # How often to check for the download completion
    start_time = time.time()

    initial_files = set(os.listdir(download_path))

    while True:
        current_files = set(os.listdir(download_path))
        new_files = current_files - initial_files
        # Check for new files with the expected extension
        if any(f.endswith(expected_extension) for f in new_files):
            return True  # Expected file found

        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            return False  # Timeout reached, download may not have completed

        time.sleep(sleep_interval)
        
def setup_selenium_browser(download_path, headless=True):
    """
    Initializes and returns a Selenium WebDriver instance with specified options.
    
    Parameters:
    - download_path: The path where downloaded files should be saved.
    - headless: Boolean indicating whether the browser should run in headless mode.
    
    Returns:
    - driver: A configured Selenium WebDriver instance.
    """
    chrome_options = Options()
    
    if headless:
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")  # Recommended as per Selenium documentation
    chrome_options.add_argument("--no-sandbox")  # Recommended for running as root/admin
    chrome_options.add_argument("--log-level=1")
    
    if download_path and download_path != "":
        # Set the download directory
        prefs = {
            "download.default_directory": download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Ensure the download directory exists
        os.makedirs(download_path, exist_ok=True)
    
    # Create the Chrome WebDriver instance
    driver = webdriver.Chrome(options=chrome_options)
    
    return driver
    
def wait_for_selenium_element(driver, css_selector, timeout=10, retries=3, parent=None):
    """Wait for an element to be present and return it, with error handling and retry for stale elements."""
    for attempt in range(retries):
        try:
            if parent:
                element = WebDriverWait(parent, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                )
            else:
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
                )
            # Quick interaction to confirm element is not stale
            _ = element.tag_name  # Accessing an attribute to confirm element is not stale
            return element
        except StaleElementReferenceException:
            if attempt < retries - 1:  # If not the last retry, just log it
                logger.info(f"Encountered a stale element, retrying... ({attempt + 1}/{retries})")
                time.sleep(1)  # Wait a bit for the DOM to stabilize
            else:  # Last attempt failed as well, log as error
                logger.error(f"Final attempt failed. Stale element reference for selector {css_selector}.")
        except TimeoutException as e:
            logger.error(f"Timeout while waiting for element with selector {css_selector}")
            return None
        except Exception as e:
            logger.error(f"Error finding element with selector {css_selector}: {str(e)}")
            return None
    return None

def execute_js_on_element(driver, element, js_script):
    """Execute JavaScript on the specified element and return the result."""
    try:
        return driver.execute_script(js_script, element)
    except Exception as e:
        logger.info(f"Error executing script on element: {str(e)}")
        return "JS execution failed"


def dataframe_to_string(df):
    # Convert the DataFrame to a CSV string, without the index and header
    # Ensure a consistent ordering of columns for consistency in hashing
    csv_string = df.sort_index(axis=1).to_csv(index=False, header=False)
    return csv_string

def hash_dataframe(df):
    # Convert the DataFrame to a consistent string representation
    csv_string = dataframe_to_string(df)
    
    # Create a hash object, update it with the string bytes, and get the hexadecimal digest
    hash_obj = hashlib.sha256()
    hash_obj.update(csv_string.encode('utf-8'))  # Encode the string to bytes
    return hash_obj.hexdigest()

def split_product_name(row):
    original_product = row['product']
    parts = original_product.replace(",", "").split()

    # Initialize default parts
    product_part, version_part, architecture_part = "", "", ""

    try:
        # Attempt to find version index with more flexible conditions
        version_index = next(i for i, part in enumerate(parts) if part[0].isdigit() and (len(part) == 4 or 'H' in part))
        product_part = " ".join(parts[:version_index])
        version_part = parts[version_index]
        architecture_part = " ".join(parts[version_index + 1:])
    except StopIteration:
        # Handle cases without a clear version number
        if 'for' in parts:
            for_index = parts.index('for')
            product_part = " ".join(parts[:for_index])
            architecture_part = " ".join(parts[for_index + 1:])
        else:
            product_part = original_product

    # Additional processing for architecture part
    if architecture_part.startswith("for "):
        architecture_part = architecture_part[4:]

    # Special handling if 'Edition' exists
    if 'Edition' in parts:
        edition_index = parts.index('Edition') + 1
        architecture_part = " ".join(parts[edition_index:])

    # Cleanup and formatting
    product_part = product_part.replace(" version", "").replace(" Version", "").replace(" Edition", "").strip()
    product_part = product_part.lower().replace(" ", "_")
    architecture_part = architecture_part.lower().replace(" ", "_").replace("(", "").replace(")", "")

    row['product_name'] = product_part
    row['product_version'] = version_part
    row['product_architecture'] = architecture_part
    
    return row


def generate_hash(*args):
    combined_string = ''.join(map(str, args))
    hash_object = hashlib.sha256(combined_string.encode())
    hash_hex = hash_object.hexdigest()
    return hash_hex
    
def add_row_guid(df, columns_to_process, col_name='id'):
    if df.empty:
        print("DataFrame is empty. Skipping GUID addition.")
        return df
    def process_row(row):
        # Concatenate the values of the specified columns
        concatenated_values = ''.join([str(row[col]) for col in columns_to_process])
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        concatenated_values = concatenated_values + timestamp
        # Generate a hash from the concatenated values
        hash_hex = generate_hash(concatenated_values)
        # Convert the hash to a GUID-like format
        guid = str(uuid.UUID(hash_hex[:32]))
        return guid
    
    # Apply the process_row function to each row and assign the result to the 'id' column
    try:
        df[col_name] = df.apply(process_row, axis=1)
    except Exception as e:
        print("Error applying process_row function:", e)
        print(df)
    return df


def convert_to_tuple(build_number):
    return tuple(map(int, build_number))

def preprocess_update_guide_product_build_data(data: pd.DataFrame, columns_to_keep: list) -> pd.DataFrame:
    """
    Preprocesses the guide's product build data from a DataFrame.

    Parameters:
    - data (pd.DataFrame): The original DataFrame containing the product build data.
    - columns_to_keep (list): A list of column names to retain in the processed DataFrame.

    Returns:
    - pd.DataFrame: A DataFrame that has been preprocessed, including renaming columns, converting data types,
      and filtering based on specific criteria.

    """
    if data.empty:
        logger.info("No raw product build data to preprocess.")
        return data

    # Keep specified columns and create a deep copy
    prepped_df = data[columns_to_keep].copy(deep=True)

    # Lowercase and replace spaces with underscores in column names
    prepped_df.columns = [col.lower().replace(' ', '_') for col in prepped_df.columns]

    # Rename columns based on presence of 'download_url'
    rename_dict = {
        'release_date': 'published',
        'details': 'cve_id',
        'article': 'kb_id',
        'download': 'package_type',
        'details_url': 'cve_url',
        'impact': 'impact_type',
        'max_severity': 'severity_type'
    }
    
    # Additional renaming if 'download_url' is present
    if 'download_url' in prepped_df.columns:
        rename_dict.update({'download_url': 'package_url'})
    
    prepped_df = prepped_df.rename(columns=rename_dict)

    # Convert 'published' column to datetime format
    prepped_df['published'] = pd.to_datetime(prepped_df['published']).dt.strftime('%d-%m-%Y')

    # Filter out rows with no or empty 'build_number' data
    prepped_df = prepped_df[prepped_df['build_number'].notna() & (prepped_df['build_number'] != '')]

    # Convert 'build_number' to a list of integers
    prepped_df['build_number'] = prepped_df['build_number'].apply(lambda x: [int(n) for n in x.split('.') if n.isdigit()])

    # Apply custom transformation (assuming split_product_name is defined elsewhere)
    prepped_df = prepped_df.apply(split_product_name, axis=1)

    # Generate 'product_build_id' based on specific columns (assuming add_row_guid is defined elsewhere)
    cols_for_product_build_id = ['product_name', 'product_version', 'product_architecture', 'build_number', 'published']
    prepped_df = add_row_guid(prepped_df, cols_for_product_build_id, 'product_build_id')

    # Sort by 'published' date in descending order
    prepped_df = prepped_df.sort_values(by='published', ascending=False)
    # print(f"preprocessed build data:\n{prepped_df.sample(n=10)}")
    # for i, row in prepped_df.iterrows():
    #     print(f"row: {row}")
    return prepped_df


def compile_update_guide_products_from_build_data(columns_to_keep: list, data: pd.DataFrame) -> pd.DataFrame:
    """
    Compile and aggregate product data from build information for an update guide. This function is used to build the "microsoft_products" table. It ensures that each product is associated with all the CVEs and KB articles that are published for that product.

    This function aggregates product build data by product name, version, and architecture. It ensures that each 
    build number is unique within its aggregation and compiles unique CVE IDs and KB IDs for each product variant. 
    The 'build_number' column is converted to a tuple to facilitate this aggregation.

    Parameters:
    - columns_to_keep (list): A list of column names to keep in the resulting DataFrame. 
      This is currently not used directly in the function but implies potential future use.
    - data (pd.DataFrame): The DataFrame containing build data for various products.

    Returns:
    - pd.DataFrame: An aggregated DataFrame with unique build numbers, CVE IDs, and KB IDs for each product variant.
    """
    
    # Convert 'build_number' to a tuple for unique aggregation
    data['build_number'] = data['build_number'].apply(convert_to_tuple)
    
    # Aggregate data with unique values in 'build_number', 'cve_id', and 'kb_id'
    aggregated_data = data.groupby(['product_name', 'product_version', 'product_architecture']).agg({
        'build_number': lambda x: [list(item) for item in set(x)],  # Ensure unique build numbers as lists
        'cve_id': lambda x: list(x.unique()),  # Collect unique CVE IDs
        'kb_id': lambda x: list(x.unique())    # Collect unique KB IDs
    }).reset_index()
    print(f"compile function put together the build data:\n{aggregated_data.head(3)}")
    return aggregated_data

def extract_filter_product_build_data(data, products_to_keep):
    
    escaped_products_to_keep = [re.escape(product) for product in products_to_keep]
    # print(f"products to keep: {products_to_keep}")
    regex_pattern = '|'.join(escaped_products_to_keep)
    # Check if the entire dataframe's hash exists in MongoDB
    # print(f"regex_patterns: {regex_pattern}")
    # remove unwanted products from dataframe before proceeding
    # For some reason, Selenium code is not correctly selecting the correct products on each run
    # print(f"rows before filter: {data.shape[0]}")
    filtered_df = data[data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
    
    return filtered_df