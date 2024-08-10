from kedro.io import AbstractDataSet
from typing import Any, Dict, List
import pprint
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import hashlib
import logging
from icecream import ic

ic.configureOutput(includeContext=True)
logger = logging.getLogger("DataSets")


class EmailDocuments(AbstractDataSet):
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

        mongo_cursor = collection.find({}, {"hash": 1, "_id": 0})
        existing_hashs = [doc["hash"] for doc in mongo_cursor]
        # ic(existing_hashs)
        new_entries = [doc for doc in data if doc["hash"] not in existing_hashs]
        print(f"new entries: {len(new_entries)}")
        if len(new_entries):
            collection.insert_many(new_entries)

        client.close()

    def _describe(self) -> Dict[str, Any]:
        return dict(
            name="EmailDocuments",
            url=self._mongo_url,
        )
