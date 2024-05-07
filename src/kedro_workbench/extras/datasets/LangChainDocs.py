from kedro.io import AbstractDataSet
from typing import Any, Dict, List
# import requests
from pymongo import MongoClient
from pymongo.errors import PyMongoError
# import hashlib
import logging
from icecream import ic

ic.configureOutput(includeContext=True)
logger = logging.getLogger("kedro")

class LangChainDocs(AbstractDataSet):
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
        # logger.debug(self._mongo_url, '\n', self._mongo_db, '\n', self._mongo_collection)
        try:
            client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            logger.debug(str(error))

        db = client[self._mongo_db]
        collection = db[self._mongo_collection]

        mongo_cursor = collection.find({}, {"metadata.id": 1, "_id": 0})
        cursor_list = list(mongo_cursor)
        if len(cursor_list):
            existing_ids = [doc["metadata"]["id"] for doc in cursor_list]
            # ic(f"cursor item: {cursor_list[-1]}")
        else:
            existing_ids = []

        new_entries = [doc for doc in data if doc["metadata"]["id"] not in existing_ids]
        # ic(new_entries[0])

        # ic(f"new entries: {len(new_entries)}")
        if len(new_entries):
            collection.insert_many(new_entries)

        client.close()

    def _describe(self) -> Dict[str, Any]:
        return dict(
            name="LangChainDocs", url=self._mongo_url, collection=self._mongo_collection
        )
