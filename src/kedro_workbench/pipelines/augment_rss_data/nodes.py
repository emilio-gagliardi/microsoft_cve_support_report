"""
This is a boilerplate pipeline 'augment_rss_data'
generated using Kedro 0.18.11
"""

from kedro_workbench.utils.feed_utils import (
    extract_link_content,
    add_id_key,
    get_new_data,
)
from typing import Dict, Any
from selenium.webdriver.chrome.options import Options
import logging
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from icecream import ic

ic.configureOutput(includeContext=True)
logger = logging.getLogger(__name__)

chrome_options = Options()
chrome_options.add_argument("--headless")

conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
catalog = conf_loader["catalog"]
credentials = credentials["mongo_atlas"]
username = credentials["username"]
password = credentials["password"]
mongo_url = f"mongodb+srv://{username}:{password}@bighatcluster.wamzrdr.mongodb.net/"


def extract_rss_1_data(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline. Kedro loads the dataset
        and passes it to the node function.
        Note. All records are feteched, needs new feature to allow
        passing of query filter.
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_rss_1_data(data, params: Dict[Any, Any]):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]
    # print("Add id key to record")
    data_with_id = add_id_key(data)
    # print("filter for just data that doesn't exist in mongo collection")
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data_with_id)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = extract_link_content(new_data, params, chrome_options)

    print(f"There are ({len(new_data)}) new MSRC documents to augment")
    # for item in new_data:
    #     print(f"{item['post_id']}-{item['revision']}-{item['published']}")
    #     print()
    return augmented_new_data


def load_rss_1_augmented(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset RSSDataSet handles the _save()
    """
    return data


def extract_rss_2_data(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_rss_2_data(data, params: Dict[Any, Any]):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]

    data_with_id = add_id_key(data)
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data_with_id)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = extract_link_content(new_data, params, chrome_options)

    return augmented_new_data


def load_rss_2_augmented(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset RSSDataSet handles the _save()
    """
    return data


def extract_rss_3_data(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_rss_3_data(data, params: Dict[Any, Any]):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]

    data_with_id = add_id_key(data)
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data_with_id)

    augmented_new_data = []

    if len(new_data):
        augmented_new_data = extract_link_content(new_data, params, chrome_options)

    return augmented_new_data


def load_rss_3_augmented(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset RSSDataSet handles the _save()
    """
    return data


def extract_rss_4_data(data):
    """extract the data from the intermediate storage collection
        pass it to the pipeline
    Args:
        data: List[json]

    Returns:
        List[json]
    """
    return data


def augment_rss_4_data(data, params: Dict[Any, Any]):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]

    data_with_id = add_id_key(data)
    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data_with_id)
    augmented_new_data = []

    if len(new_data):
        augmented_new_data = extract_link_content(new_data, params, chrome_options)

    return augmented_new_data


def load_rss_4_augmented(data):
    """
    Store the augmented json files to the augmented storage collection
    Custom Dataset RSSDataSet handles the _save()
    """
    return data
