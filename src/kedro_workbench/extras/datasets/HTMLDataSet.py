from kedro.io import AbstractDataSet
from typing import Any, Dict, List
import time
import pprint
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import hashlib
import logging
from datetime import datetime
logger = logging.getLogger(__name__)
import requests
from bs4 import BeautifulSoup
from kedro.io import AbstractDataSet
from urllib.parse import urljoin
import re
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from icecream import ic
from kedro_workbench.utils.feed_utils import (
    generate_custom_uuid,
    convert_date_string,
    remove_day_suffix,
)

chrome_options = Options()
chrome_options.add_argument("--headless")


class HTMLExtract(AbstractDataSet):
    def __init__(self, url: str, collection: str):
        self._url = url
        self._collection = collection

    def _load(self) -> dict:
        driver = webdriver.Chrome(options=chrome_options)
        try:
            driver.get(self._url)
        
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, "html.parser")
            chunks = []
            h2_divs = soup.find_all(
                "div", class_="heading-wrapper", attrs={"data-heading-level": "h2"}
            )
            # ADDED JAn 18, 2024. page format changed for Channel Notes
            time_tag = soup.find('time', attrs={'datetime': True})
            # print(f"beautiful soup found the datetime tag: {time_tag}")
            # Extract the 'datetime' attribute value if the element is found
            post_published_date = time_tag['datetime'] if time_tag else None
            # print(f"post_published_date: {post_published_date}")
            for i in range(len(h2_divs)):
                chunk = []
                current_div = h2_divs[i]
                next_div = h2_divs[i + 1] if i + 1 < len(h2_divs) else None

                # Extract the h2 element and add it to the chunk
                entry_url = current_div.find("a")
                chunk.append(entry_url)
                h2_element = current_div.find("h2")
                chunk.append(h2_element)

                # Find all elements after the current div until the next div
                sibling = current_div.find_next_sibling()
                while sibling and sibling != next_div:
                    chunk.append(sibling)
                    sibling = sibling.find_next_sibling()

                chunks.append(chunk)

            for chunk in chunks:
                for element in chunk:
                    if element.name in ("p", "div", "a", "ul", "li"):
                        self.process_a_tags(element, self._url)

            json_objects = []
            current_year = datetime.now().year
            
            for chunk in chunks:
                # print(f"chunk from edge page: {chunk}")
                a_tag = chunk[0]
                link = a_tag.get("href")
                h2_element = chunk[1]
                raw_id = h2_element.get("id")
                # ic(f"h2.id value: {raw_id}")
                date_published_raw = self.parse_date_string(raw_id)
                # ic(f"parsed from h2.id: {date_published_raw}")
                version = self.parse_version_string(raw_id)
                # ic(f"found version: {version}")
                date_published = convert_date_string(date_published_raw, current_year)
                
                # ic(f"converted date string: {date_published}")
                if date_published == "NaT":
                    continue
                    # ic(f"converted date string: {date_published}")
                published_dt = datetime.strptime(date_published, "%d-%m-%Y")
                if published_dt.year != current_year:
                    current_year = published_dt.year
                id = generate_custom_uuid(url=link, date_str=date_published)

                subject = h2_element.get_text()
                content = "".join(str(element) for element in chunk)

                # Create the JSON object
                json_object = {
                    "id": id,
                    "published": published_dt,
                    "source": link,
                    "subject": subject,
                    "page_content": content,
                    "collection": self._collection,
                    "version": version,
                }

                # Append the JSON object to the list
                json_objects.append(json_object)
        finally:
            driver.close()
            time.sleep(3)
            driver.quit()
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
            if partial_date_match.group(1).split("-")[0] not in ["january", "february", "march", "april", "may", "june", "july", "august", "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]:
                return "NaT"
            # ic(f"partial match {partial_date_match.group(1)}")
            return partial_date_match.group(1)

        # Check for an integer
        integer_pattern = r"\b(\d+)\b"
        integer_match = re.search(integer_pattern, input_string)
        ic(f"integer match: {integer_match}")
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
        self._mongo_url = f"mongodb+srv://{self._username}:{self._password}@bighatcluster.wamzrdr.mongodb.net/"

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
        # logger.debug(self._mongo_url, '\n', self._mongo_db, '\n', self._mongo_collection)
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
        ic(f"new entries: {len(new_entries)}")
        if len(new_entries):
            # print(f"new entries: {ids_to_insert}")
            collection.insert_many(new_entries)

        client.close()

    def _describe(self) -> Dict[str, Any]:
        return dict(mongo_db=self._mongo_db, mongo_collection=self._mongo_collection)
