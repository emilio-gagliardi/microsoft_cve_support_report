import json
import re
import os
from bs4 import BeautifulSoup
import fsspec
import jsonlines
import datetime
import pandas as pd
from kedro_workbench.utils.feed_utils import replace_multiple_newlines
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)
conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
credentials = credentials["mongo_atlas"]
username = credentials["username"]
password = credentials["password"]


def reshape_json(
    json_list: List[Dict], keys_for_content: List[str], keys_for_metadata: List[str]
) -> List[Dict]:
    """
    Reshapes a JSON schema from original ingested schema into a LangChain schema.

    Args:
        json_list (List[Dict]): A list of JSON objects.
        keys_for_content (List[str]): A list of keys to extract content from the JSON objects.
        keys_for_metadata (List[str]): A list of keys to extract metadata from the JSON objects.

    Returns:
        List[Dict]: A list of reshaped JSON objects with keys: page_content and metadata.
    """
    reshaped_json_list = []

    for json_obj in json_list:
        reshaped_json = {"page_content": "", "metadata": {}}

        # Preprocess page_content
        try:
            html_content = json_obj["page_content"]
            soup = BeautifulSoup(html_content, "html.parser")
            text = soup.get_text(separator=" \n")
        except KeyError:
            text = ""

        # Add text from keys_for_content in the specified order
        content_text = ""
        for key in keys_for_content:
            if key in json_obj:
                content_text += json_obj[key] + " \n"

        reshaped_json["page_content"] = content_text + replace_multiple_newlines(text)

        # Add metadata
        for key in keys_for_metadata:
            # print(f"Processing key: {key}")  # Debugging print statement
            if key in json_obj:
                # print(f"Found key {key} in json_obj.")  # Debugging print statement
                try:
                    if key == "published":
                        # Check if the value is a string before calling replace
                        if isinstance(json_obj[key], str):
                            print(f"Attempting to format {key}: {json_obj[key]}")  # Debugging print statement
                            date_str = json_obj[key].replace(" Z", "")
                            date_obj = datetime.datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S")
                            reshaped_json["metadata"][key] = date_obj.strftime("%d-%m-%Y")
                        else:
                            print(f"Expected string for {key}, but got {type(json_obj[key])}")
                            reshaped_json["metadata"][key] = json_obj[key]
                    else:
                        reshaped_json["metadata"][key] = json_obj[key]
                except AttributeError as e:
                    # Catch and print the error along with some helpful context
                    print(f"AttributeError caught while processing key {key}: {e}")

                    reshaped_json["metadata"][key] = f"{json_obj[key]}_processing_error"

        for key, value in json_obj.items():
            # Split the key by the colon (':')
            key_parts = key.split(":")
            # Check if the key has two parts
            if len(key_parts) == 2:
                # Use the first part as the key in reshaped_json
                key_str = key.split(" ")[0]
                reshaped_json["metadata"][key_str] = value

        reshaped_json_list.append(reshaped_json)

    return reshaped_json_list


def build_indexed_dictionary(data_list, prefix, base_key="index_source"):
    """
    Builds an indexed dictionary from a list of data.

    Removes the mongo db '_id' key from each entry in the data list.
    Generates a unique index key for each entry by combining the prefix, base key, and index.

    Args:
        data_list (list): A list of data to build the dictionary from.
        prefix (str): The prefix to be added to each index key.
        base_key (str, optional): The base key to be used in the index key. Defaults to "index_source".

    Returns:
        dict: The indexed dictionary built from the data list.
    """
    current_datetime = datetime.datetime.now()
    current_date_string = current_datetime.strftime("%Y-%m-%d")
    indexed_dict = {}
    print(f"building dict to save to json file with {len(data_list)}\n records")
    for idx, data in enumerate(data_list, start=1):
        # Remove the '_id' key from the data
        data.pop("_id", None)
        # print(f"{data}")
        metadata = data.get("metadata")
        if metadata.get("published"):
            if isinstance(metadata['published'], datetime.datetime):
                date_str = metadata["published"].strftime("%d-%m-%Y")
                metadata["published"] = date_str
            
        # Generate the index key
        index_key = f"{base_key}_{prefix}_{current_date_string}_{idx}"

        # Add the data to the indexed dictionary using the index key
        indexed_dict[index_key] = data

    return indexed_dict


def remove_special_characters(text):
    # Remove non-UTF-8 characters using regex
    pattern = re.compile(r"[^\x00-\x7F]")
    cleaned_text = pattern.sub("", text)
    return cleaned_text


def save_dicts_to_jsonl(azure_blob_credentials, data_list, file_path):
    fs = fsspec.filesystem("abfs", **azure_blob_credentials)

    with fs.open(file_path, "w") as outfile:
        writer = jsonlines.Writer(outfile)
        for data in data_list:
            cleaned_data = {
                k: remove_special_characters(v) if isinstance(v, str) else v
                for k, v in data.items()
            }
            writer.write(cleaned_data)


def filter_mongo_docs(documents, document_collection, mongo_db, mongo_collection):

    mongo_collection = MongoDBDocs(
        mongo_db=mongo_db,
        mongo_collection=mongo_collection,
        credentials={"username": username, "password": password},
    )
    # report docstore
    mongo_destination_collection_list = mongo_collection.find_docs(
        {"metadata.collection": f"{document_collection}"}, {"metadata.id": 1, "_id": 0}
    )
    mongo_destination_collection_ids = set([item["metadata"]["id"] for item in mongo_destination_collection_list])
    # primary collection
    mongo_source_collection_ids = set([item["metadata"]["id"] for item in documents])
    new_ids = list(mongo_source_collection_ids - mongo_destination_collection_ids)

    filtered_documents = [doc for doc in documents if doc["metadata"]["id"] in new_ids]

    return filtered_documents

def mongo_docs_to_dataframe(docs, metadata_keys=None):
    """
    Transforms a list of MongoDB documents into a Pandas DataFrame.

    Args:
        mongo_docs (list): A list of dictionaries, where each dictionary is a MongoDB document.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the MongoDB documents.
    """
    pd.set_option("display.max_rows", 600)
    pd.set_option("display.max_columns", 500)
    pd.set_option("max_colwidth", 400)
    if isinstance(metadata_keys, list) and len(metadata_keys) >= 1:
        metadata_keys = metadata_keys
    else:
        metadata_keys = None
    # print(f"metadata_keys: {metadata_keys}")
    # filtered_docs = [
    # {**({key: doc['metadata'][key] for key in (metadata_keys if metadata_keys else doc['metadata'])} if 'metadata' in doc else {}),
    #  **({'text': doc['text']} if 'text' in doc else {})}
    # for doc in docs
    # ]
    filtered_docs = [
    {
        **{key: doc['metadata'].get(key, None) for key in (metadata_keys if metadata_keys else doc['metadata'])},
        **({'text': doc['text']} if 'text' in doc else {})
    }
    for doc in docs
]
    logger.debug(f"Attempting to convert dicts to pandas with:\n{filtered_docs}")
    if len(filtered_docs) > 0:
        # Determine all possible column names
        all_column_names = set().union(*(d.keys() for d in filtered_docs))

        # Convert set of column names to list
        column_names_list = list(all_column_names)
        logger.debug(f"filtering docs with cols: {column_names_list}")
        # Create a DataFrame for the current collection with all columns
        result_df = pd.DataFrame(filtered_docs, columns=column_names_list)
        if 'revision' in result_df.columns:
            result_df['revision'] = result_df['revision'].astype(float)
            result_df['revision'] = result_df['revision'].round(2)
    else:
        logger.debug(f"mongo_docs_to_dataframe recieved 'data' as type: {type(docs)}\n")
        result_df = None
    # logger.info("finished building df.")
    # print("converting mongo docs to dataframe.")
    # for id, row in result_df.iterrows():
    #     item = row.to_dict()
    #     print(f"{item.keys()}")
    #     print()
    return result_df