"""
This is a boilerplate pipeline 'augment_patchmanagement'
generated using Kedro 0.18.11
"""
from kedro_workbench.utils.email_utils import get_day_of_week
from kedro_workbench.utils.feed_utils import get_new_data
from datetime import datetime
import re
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import logging

logger = logging.getLogger(__name__)

conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
catalog = conf_loader["catalog"]
credentials = credentials["mongo_atlas"]
username = credentials["username"]
password = credentials["password"]
mongo_url = f"mongodb+srv://{username}:{password}@bighatcluster.wamzrdr.mongodb.net/"


def extract_jsons_interm(data):
    # return dataset

    return data


def sort_jsons_interm(data):
    sorted_jsons = sorted(
        data,
        key=lambda x: (x["subject"], datetime.fromisoformat(x["receivedDateTime"])),
    )

    return sorted_jsons


def augment_jsons(data, params):
    catalog_entry = params["catalog"]
    mongo_info = catalog[catalog_entry]
    mongo_db = mongo_info["mongo_db"]
    mongo_collection = mongo_info["mongo_collection"]

    new_data = get_new_data(mongo_url, mongo_db, mongo_collection, data)

    if len(new_data):
        for json in new_data:
            subject = json["subject"]
            # Use regex to find the actual topic (without "AW:" or "Re:" prefixes)
            topic_match = re.search(r"\[patchmanagement\]\s*(.*)", subject)
            if topic_match:
                topic = topic_match.group(1)
            else:
                topic = ""
            json["topic"] = topic
            # json["day_of_week"] = get_day_of_week(json["receivedDateTime"])

    return new_data


def load_jsons_augmented(data):

    return data
