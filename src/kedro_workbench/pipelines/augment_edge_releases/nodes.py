"""
This is a boilerplate pipeline 'augment_edge_releases'
generated using Kedro 0.18.11
"""

from kedro_workbench.utils.feed_utils import (
    parse_link_content,
    compare_dates,
    get_new_data,
)
from kedro_workbench.utils.feature_engineering import get_day_of_week
from typing import Dict, Any, List, Tuple
from kedro.config import ConfigLoader
from kedro.framework.project import settings

# from pymongo import MongoClient
# from pymongo.errors import PyMongoError
import logging

logger = logging.getLogger(__name__)

conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
catalog = conf_loader["catalog"]
credentials = credentials["mongo_atlas"]
username = credentials["username"]
password = credentials["password"]
mongo_url = f"mongodb+srv://{username}:{password}@bighatcluster.wamzrdr.mongodb.net/"


def extract_stable_channel_docs(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline. Kedro loads the dataset
        and passes it to the node function.
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_stable_channel_docs(data, params):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = parse_link_content(new_data)

    return augmented_new_data


def load_stable_channel_docs(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset HTMLDataSet handles the _save()
    """
    return data


def extract_beta_channel_docs(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline. Kedro loads the dataset
        and passes it to the node function.
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_beta_channel_docs(data, params):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = parse_link_content(new_data)

    return augmented_new_data


def load_beta_channel_docs(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset HTMLDataSet handles the _save()
    """
    return data


def extract_archive_stable_channel_docs(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline. Kedro loads the dataset
        and passes it to the node function.
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_archive_stable_channel_docs(data, params):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = parse_link_content(new_data)

    return augmented_new_data


def load_archive_stable_channel_docs(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset HTMLDataSet handles the _save()
    """
    return data


def extract_mobile_stable_channel_docs(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline. Kedro loads the dataset
        and passes it to the node function.
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_mobile_stable_channel_docs(data, params):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = parse_link_content(new_data)

    return augmented_new_data


def load_mobile_stable_channel_docs(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset HTMLDataSet handles the _save()
    """
    return data


def extract_security_update_docs(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline. Kedro loads the dataset
        and passes it to the node function.
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_security_update_docs(data, params):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = parse_link_content(new_data)

    return augmented_new_data


def load_security_update_docs(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset HTMLDataSet handles the _save()
    """
    return data
