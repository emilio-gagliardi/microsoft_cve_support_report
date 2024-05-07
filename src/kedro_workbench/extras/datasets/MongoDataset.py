from kedro.io import AbstractDataSet
from typing import Any, Dict, List
import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from llama_index import Document

# BEGIN v1.0.0
class MongoDBDocs(AbstractDataSet):
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
        try:
            self._client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            self._client = None
            print(str(error))
        self._db = self._client[self._mongo_db]
        self._collection = self._db[self._mongo_collection]

    def __del__(self):
        self._client.close()

    def _load(self):
        # extract rss documents from mongo atlas
        try:
            self._client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            self._client = None
            print(str(error))
        self._db = self._client[self._mongo_db]
        self._collection = self._db[self._mongo_collection]

        cursor = self._collection.find({})
        documents_list = list(cursor)
        documents = [Document.from_dict(doc) for doc in documents_list]
        print(
            f"type container: {type(documents)} type child {type(documents[0])} num docs {len(documents)}"
        )
        self._client.close()
        return documents

    def _save(self, data: Any):
        documents_list =[]
        # Check data type and convert to list of dicts
        if isinstance(data, list) and data:
            documents_list = [doc.to_dict() for doc in data]
        elif isinstance(data, Document):
            documents_list = [data.to_dict()]
        else:
            print("Cannot convert data to list of dicts")
            
        if len(documents_list) > 0:
            # MongoDB connection setup
            try:
                self._client = MongoClient(self._mongo_url)
                self._db = self._client[self._mongo_db]
                self._collection = self._db[self._mongo_collection]
            except PyMongoError as error:
                print(str(error))
                self._client.close()
                

            # Filter out existing documents
            new_docs = []
            for doc in documents_list:
                metadata_id = doc.get("metadata", {}).get("id")
                if metadata_id and not self._collection.find_one({"metadata.id": metadata_id}):
                    print(f"Adding new document: {metadata_id}")
                    new_docs.append(doc)
                else:
                    print(f"Document already exists: {metadata_id}")

            for doc in new_docs:
                try:
                    result = self._collection.insert_one(doc)
                    if result.inserted_id:
                        print(f"Document inserted successfully with ID: {result.inserted_id}")
                    else:
                        print("Failed to insert document.")
                except pymongo.errors.DuplicateKeyError as e:
                    # Log the duplicate key error but continue processing
                    print(f"Duplicate key error for document with ID: {doc.get('metadata', {}).get('id')}. Document not inserted.")
                except Exception as e:
                    print(f"Error inserting document: {e}\n{doc}")

        # Close MongoDB client
        self._client.close()

    def _describe(self) -> Dict[str, Any]:
        return dict(
            url=self._mongo_url,
            db=self._mongo_db,
            collection=self._mongo_collection,
        )

    @property
    def client(self):
        return self._client

    @property
    def collection(self):
        return self._collection

    def find_docs(self, search_dict, projection=None):
        """
        Find documents in a MongoDB collection based on a search dictionary.
        Basic search: {"key": "value"}
        Range search: {"key": {"$gt": datetime.datetime(2009, 11, 12, 12)}}
        see https://www.mongodb.com/docs/manual/reference/operator/query/
        Args:
            search_dict: A dictionary specifying the search criteria.
            projection: A schema that determines what data is passed back to the cursor

        Returns:
            A list of dictionaries representing the found documents.
        """
        try:
            self._client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            self._client = None
            print(str(error))
        self._db = self._client[self._mongo_db]
        self._collection = self._db[self._mongo_collection]
        # Check if search_dict is not a dictionary, and set it to an empty dictionary if so
        if not isinstance(search_dict, dict):
            search_dict = {}
        if projection is not None:
            # Check if projection is a dictionary
            if not isinstance(projection, dict):
                # print("projection is not a dictionary")
                projection = None
        if projection is not None:
            # print(f"find_docs() searching for: {search_dict} with projection: {projection}")
            cursor = self._collection.find(search_dict, projection)

        else:
            # print(f"find_docs() searching for: {search_dict} without projection")
            cursor = self._collection.find(search_dict)

        documents = list(cursor)

        return documents

    def find_sort_docs(self, search_dict, sort_list, projection=None):
        """
        Find documents in a MongoDB collection based on a search dictionary and sorts the results according to a sort dictionary.
        Basic search: {"key": "value"}
        Range search: {"key": {"$gt": datetime.datetime(2009, 11, 12, 12)}}
        see https://www.mongodb.com/docs/manual/reference/operator/query/

        sort_list: [("key", pymongo.ASCENDING), ("key", pymongo.DESCENDING)]
        Args:
            search_dict: A dictionary specifying the search criteria.
            sort_list: A list of tuples specifying the sort criteria.
            projection: A schema that determines what data is passed back to the cursor

        Returns:
            A list of found documents that are sorted.
        """
        try:
            self._client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            self._client = None
            print(str(error))
        self._db = self._client[self._mongo_db]
        self._collection = self._db[self._mongo_collection]

        if not isinstance(search_dict, dict):
            search_dict = {}
        if not isinstance(sort_list, list) or len(sort_list) == 0:
            # Variable is not a list or it's an empty list
            # Create a list and insert a tuple
            sort_list = [("published", pymongo.DESCENDING)]
        elif not any(isinstance(item, tuple) for item in sort_list):
            # Variable is a list, but it doesn't contain any tuple
            # Insert a tuple
            sort_list.append(("published", pymongo.DESCENDING))
        if projection is not None:
            # Check if projection is a dictionary
            if not isinstance(projection, dict):
                print("projection is not a dictionary")
                projection = None
                
        # print("Projection:", projection)
        # print("Search Dict:", search_dict)
        # print("Sort List:", sort_list)
        if projection is not None:
            cursor = self._collection.find(search_dict, projection).sort(sort_list)
        else:
            cursor = self._collection.find(search_dict).sort(sort_list)
        documents = list(cursor)
        # for doc in documents[:1]:
        #     print(doc)
        return documents

    def count_docs(self, search_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(search_dict, dict):
            search_dict = {}
        try:
            self._client = MongoClient(self._mongo_url)
        except PyMongoError as error:
            self._client = None
            print(str(error))
        self._db = self._client[self._mongo_db]
        self._collection = self._db[self._mongo_collection]
        count = self._collection.count_documents(search_dict)

        return count

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

# END v1.0.0

# BEGIN v2.0.0
class MongoDataset(AbstractDataSet):
    def __init__(self, mongo_db: str, mongo_collection: str, credentials: Dict[str, Any]):
        self._mongo_db = mongo_db
        self._mongo_collection = mongo_collection
        self._client = None
        self._db = None
        self._collection = None
        self._credentials = credentials
        self._username = credentials["username"]
        self._password = credentials["password"]
        self._mongo_url = f"mongodb+srv://{self._username}:{self._password}@bighatcluster.wamzrdr.mongodb.net/"

    def _describe(self) -> Dict[str, Any]:
        return dict(mongo_url=self._mongo_url, mongo_db=self._mongo_db, mongo_collection=self._mongo_collection)

    def _load(self) -> Any:
        self._client = MongoClient(self._mongo_url)
        self._db = self._client[self._mongo_db]
        self._collection = self._db[self._mongo_collection]
        return list(self._collection.find())

    def _save(self, data: Any) -> None:
        self._client = MongoClient(self._mongo_url)
        self._db = self._client[self._mongo_db]
        self._collection = self._db[self._mongo_collection]
        self._collection.insert_many(data)

    def _exists(self) -> bool:
        self._client = MongoClient(self._mongo_url)
        self._db = self._client[self._mongo_db]
        return self._mongo_collection in self._db.list_collection_names()