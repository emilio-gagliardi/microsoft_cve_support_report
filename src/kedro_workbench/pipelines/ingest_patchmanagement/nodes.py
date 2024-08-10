"""
This is a boilerplate pipeline 'ingest_patchmanagement'
generated using Kedro 0.18.11
"""

from typing import Any, Dict, List

import hashlib
import logging
from datetime import datetime
from tqdm import tqdm

# import fsspec
# from icecream import ic
from kedro.config import ConfigLoader
from kedro.framework.project import settings

# from kedro_workbench.utils.decorators import timing_decorator
from kedro_workbench.utils.email_utils import (
    clean_email,
    convert_date_string,
    move_files_between_containers,
    write_empty_json_to_blob_container,
)
from kedro_workbench.utils.feed_utils import (
    generate_custom_uuid,
    replace_multiple_newlines,
)

logger = logging.getLogger(__name__)


# ic.configureOutput(includeContext=True)


def extract_partitioned_json(partitioned_jsons) -> Dict[str, Any]:
    # no processing required
    logger.info("Extracting patch management emails from blob storage.")
    return partitioned_jsons


def combine_partitioned_json(partitioned_jsons) -> List[Dict[str, Any]]:
    # For patch management, we add the hash in the node logic similar to rss data.
    merged_list = []

    for partition_key, partition_load_func in sorted(partitioned_jsons.items()):
        # print(partition_key)
        try:
            list_of_json = partition_load_func()
        except Exception as e:
            logger.error(f"Error loading patch management json {partition_key}: {e}")

        if not list_of_json:
            continue
        keys_to_hash = [
            "source",
            "conversationId",
            "subject",
            "from",
            "receivedDateTime",
        ]
        for json in tqdm(list_of_json):
            json["source"] = partition_key
            published_str = convert_date_string(json["receivedDateTime"])
            json["published"] = datetime.strptime(published_str, "%d-%m-%Y")
            dt = datetime.fromisoformat(json["receivedDateTime"])
            version = f"{json['conversationId']}:{dt.hour}:{dt.minute}:{dt.second}"
            json["id"] = generate_custom_uuid(
                url=json["source"],
                version=version,
                date_str=published_str,
                collection="patch_management",
            )
            subset_dict = {key: json[key] for key in keys_to_hash if key in json}
            dict_hash = hashlib.sha256(
                str(sorted(subset_dict.items())).encode()
            ).hexdigest()
            json["hash"] = dict_hash
            merged_list.append(json)
    logger.info(
        f"Total patch managment emails pulled from blob storage" f"{len(merged_list)}"
    )
    return merged_list


def clean_jsons(combined_jsons):
    combined_cleaned_sorted = []
    logger.info("Start basic string and pattern-based cleaning")

    for i, item in enumerate(tqdm(combined_jsons, desc="Processing email body text")):
        item["body"] = clean_email(item["body"])
        item["body"] = replace_multiple_newlines(item["body"])

    combined_cleaned_sorted = sorted(
        combined_jsons,
        key=lambda x: (x["subject"], datetime.fromisoformat(x["receivedDateTime"])),
    )

    return combined_cleaned_sorted


def load_jsons(cleaned_jsons):

    return cleaned_jsons


def move_jsons_azure(data, params):
    conf_loader = ConfigLoader(settings.CONF_SOURCE)
    credentials = conf_loader["credentials"]
    azure_blob_credentials = credentials["azure_blob_credentials"]

    source_path = params["source_path"]
    target_path = params["target_path"]
    list_of_files = [item["source"] for item in data]
    # ic(list_of_files)
    move_files_between_containers(
        azure_blob_credentials, source_path, target_path, list_of_files
    )


def add_placeholder(params):
    # description: built-in kedro partitioned dataset expects
    # at least 1 item in the directory. This is a workaround to prevent
    # the next run from crashing
    conf_loader = ConfigLoader(settings.CONF_SOURCE)
    credentials = conf_loader["credentials"]
    azure_blob_credentials = credentials["azure_blob_credentials"]
    placeholder_path = params["source_path"]
    write_empty_json_to_blob_container(azure_blob_credentials, placeholder_path)
