import hashlib
import logging
# import pprint
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin

# import requests
from bs4 import BeautifulSoup
# from icecream import ic
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.keys import Keys
from typing import Any, Dict

from kedro.io import AbstractDataSet
from kedro_workbench.utils.feed_utils import (
    generate_custom_uuid,
    remove_day_suffix,
)

logger = logging.getLogger(__name__)

chrome_options = Options()
chrome_options.add_argument("--headless")


class HTMLExtract(AbstractDataSet):
    def __init__(self, url: str, collection: str, day_interval=None):
        self._url = url
        self._collection = collection
        self._day_interval = day_interval

    def _load(self) -> dict:
        # Initialize WebDriver to fetch page content
        driver = webdriver.Chrome(options=chrome_options)
        try:
            # Retrieve the HTML content from the specified URL
            driver.get(self._url)
            # print(self._url)
            logger.info(f"Extracting content from URL: {self._url}")
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, "html.parser")

            # Find all H2 headings within the specified div tags
            # h2_divs = soup.find_all("div",
            #                         class_="heading-wrapper",
            #                         attrs={"data-heading-level": "h2"}
            #                         )

            h2_divs = soup.select('div.heading-wrapper[data-heading-level="h2"]')

            # Calculate cutoff date from the current date
            # based on the specified day_interval
            # cutoff_date = (
            #     datetime.now() - timedelta(days=self._day_interval)
            #     if self._day_interval is not None
            #     else None
            # )
            cutoff_date = datetime.now() - timedelta(days=self._day_interval or 0)

            # Initialize list to store processed data chunks
            chunks = []

            # Iterate through found H2 divs to extract and process information
            for i in range(len(h2_divs)):
                current_div = h2_divs[i]
                next_div = h2_divs[i + 1] if i + 1 < len(h2_divs) else None
                chunk = []

                # Capture the link and H2 element details
                entry_url = current_div.find("a")
                h2_element = current_div.find("h2")
                chunk.extend([entry_url, h2_element])

                # Gather all relevant sibling elements till the next div
                sibling = current_div.find_next_sibling()
                while sibling and sibling != next_div:
                    chunk.append(sibling)
                    sibling = sibling.find_next_sibling()

                chunks.append(chunk)

            # Process each chunk to extract and format required 
            # information into JSON objects
            json_objects = []
            # current_year = datetime.now().year
            for chunk in chunks:
                a_tag, h2_element = chunk[0], chunk[1]
                link = a_tag.get("href")
                raw_id = h2_element.get("id")
                date_published_raw = self.parse_date_string(raw_id)
                # print(f"date_published_raw is: {date_published_raw}")
                version = self.parse_version_string(raw_id)

                # Validate complete date; skip processing if 'NaT' or incomplete
                if date_published_raw == "NaT" or not re.search(r'\d{4}', date_published_raw):
                    continue

                # date_published = convert_date_string(date_published_raw, current_year)
                published_dt = datetime.strptime(date_published_raw, "%B-%d-%Y")

                # Filter data based on the cutoff date if day_interval is specified
                if cutoff_date is not None and published_dt < cutoff_date:
                    continue

                id = generate_custom_uuid(url=link, date_str=date_published_raw)
                subject = h2_element.get_text()
                content = "".join(str(element) for element in chunk)

                # Create JSON object with extracted and formatted data
                json_object = {
                    "id": id,
                    "published": published_dt,
                    "source": link,
                    "subject": subject,
                    "page_content": content,
                    "collection": self._collection,
                    "version": version,
                }
                json_objects.append(json_object)

        finally:
            # Ensure the WebDriver is closed properly
            driver.close()
            time.sleep(3)
            driver.quit()
        # for item in json_objects:
        #     print(item)
        logger.info(f"Finished extracting {len(json_objects)} items for ingestion.")
        return json_objects

    def _save(self, data: dict):
        raise NotImplementedError("Saving HTML data is not supported.")

    def _describe(self) -> dict:
        return {
            "url": self._url,
        }

    def process_a_tags(self, element, base_url):
        if element.name == "a":
            href = element.get("href")
            if href and href.startswith(("/", "#")):
                absolute_url = urljoin(base_url, href)
                element["href"] = absolute_url
        else:
            a_tags = element.find_all("a")
            for a_tag in a_tags:
                href = a_tag.get("href")
                if href and href.startswith(("/", "#")):
                    absolute_url = urljoin(base_url, href)
                    a_tag["href"] = absolute_url

    def parse_date_string(self, input_string):
        # Check for a complete date format
        # ic(f"parsing string for date {input_string}") # september-15th-2023
        input_string = remove_day_suffix(input_string)
        complete_date_pattern = r"\b([a-zA-Z]+(?:-\d{1,2})+-\d{4})\b"
        complete_date_match = re.search(complete_date_pattern, input_string)
        if complete_date_match:
            # ic(f"complete match {complete_date_match.group(1)}")
            return complete_date_match.group(1)

        # Check for a partial date format
        partial_date_pattern = r"\b([a-zA-Z]+(?:-\d{1,2})+)\b"
        partial_date_match = re.search(partial_date_pattern, input_string)
        if partial_date_match:
            months = [
                "january", "february", "march", "april", "may", "june", 
                "july", "august", "september", "october", "november", "december",
                "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct",
                "nov", "dec"
            ]
            if partial_date_match.group(1).split("-")[0].lower() not in months:
                return "NaT"
            # ic(f"partial match {partial_date_match.group(1)}")
            return partial_date_match.group(1)

        # Check for an integer
        # not sure what this portion is needed for...
        integer_pattern = r"\b(\d+)\b"
        integer_match = re.search(integer_pattern, input_string)

        if integer_match and not input_string.isdigit():
            return "NaT"
        # ic(f"couldnt parse a date from {input_string}")
        return "NaT"

    def parse_version_string(self, input_string):
        match = re.search(r'version-(\d+)-', input_string)
        if match:
            return match.group(1)

        return None


class HTMLDocuments(AbstractDataSet):
    def __init__(
        self,
        mongo_db: str,
        mongo_collection: str,
        credentials: Dict[str, Any],
        data: Any = None,
    ):
        self._data = None
        self._mongo_db = mongo_db
        self._mongo_collection = mongo_collection
        self._username = credentials["username"]
        self._password = credentials["password"]
        self._mongo_url = (
            f"mongodb+srv://{self._username}:"
            f"{self._password}@bighatcluster.wamzrdr.mongodb.net/"
            )

    def _load(self):
        # extract rss documents from mongo atlas
        try:
            client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            logger.debug(str(error))

        db = client[self._mongo_db]
        collection = db[self._mongo_collection]
        cursor = collection.find({})
        documents = list(cursor)
        # logger.info(f"records: {len(documents)}")
        return documents

    def _save(self, data: Any):

        try:
            client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            logger.debug(str(error))

        db = client[self._mongo_db]
        collection = db[self._mongo_collection]

        keys_to_hash = ["source", "subject", "published", "collection"]

        for doc in data:
            if "hash" not in doc:
                subset_dict = {key: doc[key] for key in keys_to_hash if key in doc}
                dict_hash = hashlib.sha256(
                    str(sorted(subset_dict.items())).encode()
                ).hexdigest()
                doc["hash"] = dict_hash
        mongo_cursor = collection.find({}, {"hash": 1, "_id": 0})
        existing_hashs = [doc["hash"] for doc in mongo_cursor]
        # ic(existing_hashs)
        new_entries = [doc for doc in data if doc["hash"] not in existing_hashs]
        ids_to_insert = [doc["id"] for doc in new_entries]
        logger.info(f"Loaded {len(new_entries)} new entries in {self._mongo_collection}")
        if len(new_entries):
            logger.info(f"Inserting IDs: {ids_to_insert}")
            collection.insert_many(new_entries)

        client.close()

    def _describe(self) -> Dict[str, Any]:
        return dict(mongo_db=self._mongo_db, mongo_collection=self._mongo_collection)
