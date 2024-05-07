"""
This is a boilerplate pipeline 'transform_documents'
generated using Kedro 0.18.11
"""
from kedro_workbench.utils.json_utils import reshape_json
from kedro_workbench.utils.feed_utils import get_new_data
from bs4 import BeautifulSoup
import json
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from pymongo import MongoClient
from pymongo.errors import PyMongoError

conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
catalog = conf_loader["catalog"]
credentials = credentials["mongo_atlas"]
username = credentials["username"]
password = credentials["password"]
mongo_url = f"mongodb+srv://{username}:{password}@bighatcluster.wamzrdr.mongodb.net/"


def extract_rss_1_docs(data):

    return data


def transform_rss_1(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_rss_1(data):

    return data


def extract_rss_2_docs(data):

    return data


def transform_rss_2(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_rss_2(data):

    return data


def extract_rss_3_docs(data):

    return data


def transform_rss_3(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_rss_3(data):

    return data


def extract_rss_4_docs(data):

    return data


def transform_rss_4(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_rss_4(data):

    return data


def extract_edge_1_docs(data):
    # print(f"extract: {len(data)}")
    return data


def transform_edge_1(data, params):
    # print(f"pre-transform: {len(data)}")
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_edge_1(data):

    return data


def extract_edge_2_docs(data):

    return data


def transform_edge_2(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_edge_2(data):

    return data


def extract_edge_3_docs(data):

    return data


def transform_edge_3(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_edge_3(data):

    return data


def extract_edge_4_docs(data):

    return data


def transform_edge_4(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_edge_4(data):

    return data


def extract_edge_5_docs(data):

    return data


def transform_edge_5(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []

    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_transformed_edge_5(data):

    return data


def extract_patchmanagement_docs(data):

    return data


def transform_patchmanagement_docs(data, params):
    keys_for_content = params["keys_for_content"]
    keys_for_metadata = params["keys_for_metadata"]
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    print("transform patch management docs")
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data, nested_id=True)
    transformed_new_data = []
    print(f"len new data {len(new_data)}")
    if len(new_data):
        transformed_new_data = reshape_json(
            new_data, keys_for_content, keys_for_metadata
        )
        # print(transformed_new_data[0])
    return transformed_new_data


def load_patchmanagement_docs(data):

    return data, True

def begin_consolidating_pipeline_connector(consolidated_10):
    
    return True
