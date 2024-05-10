from kedro.io import AbstractDataSet
from typing import Any, Dict, List
from kedro_workbench.utils.feed_utils import (
    generate_feed_structure_text,
    generate_feed_structure_yaml,
)
import pprint
import feedparser
from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import hashlib
import logging
from icecream import ic
from datetime import datetime, timedelta
ic.configureOutput(includeContext=True)
logger = logging.getLogger("kedro")


class RSSFeedExtract(AbstractDataSet):
    def __init__(self, url, collection, day_interval=None, output_format="text", output_dir=None):
        self._url = url
        self._collection = collection
        self._day_interval = day_interval
        self._schema_format = output_format
        self._schema_dir = output_dir
        self._feed_name = None

    @property
    def feed_name(self):
        return self._feed_name

    @feed_name.setter
    def feed_name(self, value):
        self._feed_name = value

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, value):
        self._url = value

    @property
    def schema_dir(self):
        return self._schema_dir

    @schema_dir.setter
    def schema_dir(self, value):
        self._schema_dir = value

    def _load(self):
        """ Load data from RSS feed, filtering items based on publication date. """
        response = requests.get(self.url)
        xml_content = response.text
        soup = BeautifulSoup(xml_content, "xml")
        items = soup.find_all("item")

        if self._day_interval is not None:
            cutoff_date = datetime.now() - timedelta(days=self._day_interval)

        parsed_feed = []
        collection = self._collection
        for item in items:
            item_dict = {}

            guid = item.find("guid")
            if guid:
                item_dict["post_id"] = guid.text

            link = item.find("link")
            if link:
                item_dict["link"] = link.text

            category = item.find("category")
            if category:
                item_dict["category"] = category.text

            title = item.find("title")
            if title:
                item_dict["title"] = title.text

            description = item.find("description")
            if description:
                item_dict["description"] = description.text

            pub_date = item.find("pubDate")
            if pub_date:
                item_dict["published"] = pub_date.text
                # Convert pubDate to datetime object for comparison
                # print(f"raw date string: {pub_date.text}")
                pub_date_dt = datetime.strptime(pub_date.text.replace('Z', 'UTC'), '%a, %d %b %Y %H:%M:%S %Z')

                # Filter based on the cutoff_date if days_ago is not None
                if self._day_interval is not None and pub_date_dt < cutoff_date:
                    continue

            item_dict["collection"] = collection
            parsed_feed.append(item_dict)
        # for item in parsed_feed:
        #     print(f"{item}")
        return parsed_feed

    def _save(self, data: Any) -> None:
        raise NotImplementedError("Saving RSS feed data is not supported.")

    def _describe(self) -> Dict[str, Any]:
        return dict(url=self._url)


class RSSDocuments(AbstractDataSet):
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

        return documents

    def _save(self, data: Any):
        try:
            client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            logger.debug(str(error))
            return

        db = client[self._mongo_db]
        collection = db[self._mongo_collection]

        mongo_cursor = collection.find({}, {"hash": 1, "_id": 0})
        existing_hashes = [doc["hash"] for doc in mongo_cursor]
        new_entries = []

        for doc in data:
            if doc["hash"] not in existing_hashes:
                # Convert the 'published' field from string to datetime object
                # print(f"augmented doc to insert into mongo: {doc}")
                new_entries.append(doc)

        if new_entries:
            print("adding those augmented documents.")
            try:
                results = collection.insert_many(new_entries)
                # print(f"Documents inserted, IDs: {results.inserted_ids}")
            except Exception as e:
                print(f"Error occurred during insert_many: {e}")
            
        client.close()

    def _describe(self) -> Dict[str, Any]:
        return dict(
            name="RSSDocuments",
            url=self._mongo_url,
            username=self._username,
            password=self._password,
        )
