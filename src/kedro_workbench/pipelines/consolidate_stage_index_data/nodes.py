"""
This is a boilerplate pipeline 'consolidate_stage_index_data'
generated using Kedro 0.18.11
Pipeline does not create duplicates if json files are moved after processing within 1 day. Jan 19, 2024.
"""

from kedro_workbench.utils.json_utils import (
    build_indexed_dictionary,
    save_dicts_to_jsonl,
    filter_mongo_docs,
)
from kedro.config import ConfigLoader
from kedro.framework.project import settings
import logging

logger = logging.getLogger(__name__)
conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
credentials = credentials["mongo_atlas"]
username = credentials["username"]
password = credentials["password"]


def extract_rss_1_primary(data, document_limit):
    """
    Extracts the rss_1 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "msrc_security_update", "report_docstore", "docstore"
    )

    logger.info(
        f"rss_1_primary collection: {len(data)} records. rss_1 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_rss_2_primary(data, document_limit):
    """
    Extracts the rss_2 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "windows_update", "report_docstore", "docstore"
    )
    logger.info(
        f"rss_2_primary collection: {len(data)} records. rss_2 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_rss_3_primary(data, document_limit):
    """
    Extracts the rss_3 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "windows_10", "report_docstore", "docstore"
    )
    logger.info(
        f"rss_33_primary collection: {len(data)} records. rss_3 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_rss_4_primary(data, document_limit):
    """
    Extracts the rss_4 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "windows_11", "report_docstore", "docstore"
    )
    logger.info(
        f"rss_4_primary collection: {len(data)} records. rss_4 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_edge_1_primary(data, document_limit):
    """
    Extracts the edge_1 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "stable_channel_notes", "report_docstore", "docstore"
    )
    logger.info(
        f"edge_1_primary collection: {len(data)} records. edge_1 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_edge_2_primary(data, document_limit):
    """
    Extracts the edge_2 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "beta_channel_notes", "report_docstore", "docstore"
    )
    logger.info(
        f"edge_2_primary collection: {len(data)} records. edge_2 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_edge_3_primary(data, document_limit):
    """
    Extracts the edge_3 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "archive_stable_channel_notes", "report_docstore", "docstore"
    )
    logger.info(
        f"edge_3_primary collection: {len(data)} records. edge_3 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_edge_4_primary(data, document_limit):
    """
    Extracts the edge_4 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "mobile_stable_channel_notes", "report_docstore", "docstore"
    )
    logger.info(
        f"edge_4_primary collection: {len(data)} records. edge_4 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_edge_5_primary(data, document_limit):
    """
    Extracts the edge_5 documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "security_update_notes", "report_docstore", "docstore"
    )
    logger.info(
        f"edge_5_primary collection: {len(data)} records. edge_5 ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


def extract_patch_primary(data, document_limit):
    """
    Extracts the patch management google group documents from the primary collection
    via a custom dataset specified in the catalog.yml.
    See pipeline.py for the dataset name.

    Args:
        data (list): The input data to be processed.

    Returns:
        list: The data extracted from the primary collection.
    """
    if isinstance(document_limit, str) and document_limit == "None":
        document_limit = None
    filtered_documents = filter_mongo_docs(
        data, "patch_management", "report_docstore", "docstore"
    )
    logger.info(
        f"patch_primary collection: {len(data)} records. patch ids not in docstore: {len(filtered_documents)}"
    )

    return filtered_documents[:document_limit]


# EACH LOAD NODE GENERATES A DICTIONARY WHERE EACH KEY IS A PARTITION KEY (USED FOR FILENAME) AND
# THE VALUE IS THE JSON OBJECT FOR THAT RECORD. THERE IS ONE JSON FOR EVERY RECORD
# TO INSERT INTO THE DOCSTORE.
# FILES NEED TO BE MOVED ONCE PROCESSED TO A DIFFERENT DIRECTORY TO AVOID REPROCESSING
# {'index_source_stable_2024-01-20_1': {'page_content': 'Version 120.0.2210.133: January 11, 2024 \nVersion 120.0.2210.133: January 11, 2024 \nFixed various bugs and performance issues. \nStable channel security updates are listed  \nhere \n.', 'metadata': {'id': '19db971a-9bdb-0bae-3095-dcae81bb6470', 'source': 'https://learn.microsoft.com/en-us/deployedge/microsoft-edge-relnote-stable-channel#version-12002210133-january-11-2024', 'subject': 'Version 120.0.2210.133: January 11, 2024', 'collection': 'stable_channel_notes', 'published': '11-01-2024', 'security_link:here': 'https://learn.microsoft.com/en-us/deployedge/microsoft-edge-relnotes-security#january-11-2024'}}, 'index_source_stable_2024-01-20_2': {'page_content': "Version 120.0.2210.89: December 20, 2023 \nVersion 120.0.2210.89: December 20, 2023 \nFixed various bugs and performance issues. \nFeature updates \nMicrosoft Edge Workspaces improvements for offline functionality.", 'metadata': {'id': '57be3be6-de42-a048-e09b-8808a4551235', 'source': 'https://learn.microsoft.com/en-us/deployedge/microsoft-edge-relnote-stable-channel#version-1200221089-december-20-2023', 'subject': 'Version 120.0.2210.89: December 20, 2023', 'collection': 'stable_channel_notes', 'published': '20-12-2023', 'content_link:display_text_2': 'https://learn.microsoft.com/en-us/deployedge/microsoft-edge-relnote-stable-channel#feature-updates'}}}

# TODO. Add document hash to document schema and check document hashes instead of
# requiring to move files after loading


def load_rss_1_index(data, params):
    """
    Save the preprocessed RSS 1 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num rss_1 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    # print(f"payload: {payload}")
    return payload


def load_rss_2_index(data, params):
    """
    Save the preprocessed RSS 2 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num rss_2 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    # print(f"payload: {payload}")
    return payload


def load_rss_3_index(data, params):
    """
    Save the preprocessed RSS 3 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num rss_3 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    return payload


def load_rss_4_index(data, params):
    """
    Save the preprocessed RSS 4 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num rss_4 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    return payload


def load_edge_1_index(data, params):
    """
    Save the preprocessed Edge 1 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num edge_1 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    # print(f"payload: {payload}")
    return payload


def load_edge_2_index(data, params):
    """
    Save the preprocessed Edge 2 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num edge_2 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    # print(f"payload: {payload}")
    return payload


def load_edge_3_index(data, params):
    """
    Save the preprocessed Edge 3 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num edge_3 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    # print(f"payload: {payload}")
    return payload


def load_edge_4_index(data, params):
    """
    Save the preprocessed Edge 4 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num edge_4 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    return payload


def load_edge_5_index(data, params):
    """
    Save the preprocessed Edge 5 documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num edge_5 to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    return payload


def load_patch_index(data, params):
    """
    Save the preprocessed patch management documents to the target directory via a custom dataset.
    See pipeline.py for the dataset name.

    Args:
        data (dict): The data to be used for loading the index.
        params (dict): The parameters for loading the index. It should contain the following keys:
            - prefix (str): The prefix for the index.
            - base (str): The base for the index.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
        Boolean: memory dataset ingested by node later in pipeline
    """
    print(f"num patch to load: {len(data)}")
    prefix = params["prefix"]
    base = params["base"]
    payload = build_indexed_dictionary(data, prefix, base)
    return payload


def extract_partitioned_index_source(data):
    """
    Extract the partitioned json files from the target directory via a custom dataset.
    Note. Function not currently called in the pipeline.

    Args:
        data (dict): The custom dataset that extracts the partitioned json files
        from the directory specified in the catalog.

    Returns:
        List[dict]: The list of rehspaped JSON objects to save to disk.
    """
    return data


def combine_partitioned_index_source(data):
    """
    Combine the partitioned index source data into a single list.
    Note. Function not currently called in the pipeline.

    Args:
        data (dict): A dictionary containing the partition keys as keys and the partition load functions as values.

    Returns:
        list: A merged list containing the results of the partition load functions.

    Raises:
        Exception: If there is an error loading a partition.

    """
    merged_list = []

    for partition_key, partition_load_func in sorted(data.items()):
        # print(partition_key)
        try:
            json = partition_load_func()
        except Exception as e:
            print(f"Error loading {partition_key}: {e}")

        merged_list.append(json)

    return merged_list


def load_index_jsonl(data, params):
    """
    Load the JSONL index file with the provided data and parameters.
    Note. Function not currently called in the pipeline.

    Args:
        data (list): The data to be loaded into the index file.
        params (dict): The parameters for loading the index file.

    Returns:
        None
    """
    conf_loader = ConfigLoader(settings.CONF_SOURCE)
    credentials = conf_loader["credentials"]
    azure_blob_credentials = credentials["azure_blob_credentials"]

    file_path = params["file_path"]
    sample_set = data[:9]
    save_dicts_to_jsonl(azure_blob_credentials, sample_set, file_path)
    return data
