"""
This is a boilerplate pipeline 'update_index'
generated using Kedro 0.18.11
"""

from llama_index import Document
from llama_index.callbacks import CallbackManager, LlamaDebugHandler
from llama_index.node_parser import SentenceWindowNodeParser
from llama_index.text_splitter import SentenceSplitter

import tiktoken
from fsspec import filesystem
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Tuple, Callable
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from kedro_workbench.utils.llm_utils import (
    find_matching_keys,
    find_cve_patterns,
    convert_date_string,
    prepare_for_sentence_parser_llama_sentence,
)

from kedro_workbench.utils.decorators import timing_decorator
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

llama_debug = LlamaDebugHandler(print_trace_on_end=True)
callback_manager = CallbackManager([llama_debug])


@timing_decorator
def get_partitioned_index_data(data):
    """
    Load a partitioned dataset from a directory.

    Args:
        data (dict): A dictionary containing the data to be partitioned.

    Returns:
        dict: A dictionary containing the file names of the data to load.

    """
    developing = False
    # print(len(data))

    if developing:
        import itertools

        sliced_dict = dict(itertools.islice(data.items(), 15))
        return sliced_dict
    else:
        return data


@timing_decorator
def combine_callable_index_data(data: Dict[str, Callable[[], dict]]) -> List[dict]:
    """
    Combines the data retrieved from the partitioned dataset
    into a single list of dictionaries.

    Args:
        data: A dictionary containing partition keys as keys
              and callable functions as values.

    Returns:
        A list containing the loaded json files.
    """
    merged_list: List[dict] = []
    files_to_remove = []
    if not data:
        print("Dictionary is empty. No data to preprocess.")
    else:
        print("Dictionary is not empty.")

    for partition_key, partition_load_func in tqdm(
        sorted(data.items()), desc="Processing combinables..."
    ):
        file_to_remove = f"{partition_key}"
        files_to_remove.append(file_to_remove)
        try:
            json: dict = (
                partition_load_func()
            )  # Call the callable function to retrieve data
        except Exception as e:
            print(
                f"Error loading {partition_key}: {e}"
            )  # Print error message if loading data fails

        merged_list.append(json)  # Append the retrieved data to the merged list

    return merged_list, files_to_remove


@timing_decorator
def create_documents_from_index_data(data: List[Dict[str, Any]]) -> List[Document]:
    """
    Create a list of llama Index document objects from the given data.

    Args:
        data: A list of dictionaries representing the index data.

    Returns:
        A list of langchain document objects created from the index data.
    """

    documents = [
        Document(
            id_=data_dict["metadata"]["id"],
            text=data_dict["page_content"],
            metadata={**data_dict["metadata"]},
            excluded_llm_metadata_keys=[
                "id",
                "added_to_vector_store",
                "added_to_summary_index",
                "added_to_graph_store",
                "cve_fixes",
                "cve_mentions",
                "email_text_original",
                "unique_tokens",
            ],
            excluded_embed_metadata_keys=[
                "id",
                "added_to_vector_store",
                "added_to_summary_index",
                "added_to_graph_store",
                "cve_fixes",
                "cve_mentions",
                "email_text_original",
                "unique_tokens",
            ],
        )
        for data_dict in data
    ]

    for document in tqdm(documents, desc="creating Documents from json data..."):
        if document.metadata.get("published") is None:
            now = datetime.now()  # get current system time
            formatted_date = now.strftime("%d-%m-%Y")
            document.metadata["published"] = None
        else:
            document.metadata["published"] = convert_date_string(
                document.metadata["published"]
            )
        if document.metadata["published"] is not None:
            published_dt = datetime.strptime(document.metadata["published"], "%d-%m-%Y")
            document.metadata["published"] = published_dt
        else:
            print(f"document {document.metadata['id']} has a bad date string")

        if document.metadata.get("collection") is None:
            document.metadata["collection"] = "patch_management"
        document.metadata["added_to_vector_store"] = False
        document.metadata["added_to_summary_index"] = False
        document.metadata["added_to_graph_store"] = False
        # del document.metadata["day_of_week"]
        # del document.metadata["hash"]

        # named_entities = extract_entities(document.text)
        # cleaned_entities_set = set(clean_named_entities(named_entities))
        # nouns_set = set(extract_nouns(document.text))
        # keywords_set = nouns_set.union(cleaned_entities_set)
        # keywords_list = list(keywords_set)
        # cleaned_keyword_list = remove_keywords(keywords_list)

        # print(f"keywords set: {keywords_set}")
        # keywords_str = ", ".join(cleaned_keyword_list)
        # document.metadata["keywords"] = keywords_str

        # print(f"doc: {document.metadata['id']} document keywords: {document.metadata['keywords']}\n")
        fixes_str = ""
        updates_str = ""
        fixes_list, updates_list = find_cve_patterns(document.text)
        fixes_str = " ".join(fixes_list)
        document.metadata["cve_fixes"] = fixes_str

        updates_str = " ".join(updates_list)
        document.metadata["cve_mentions"] = updates_str
        # print(document.metadata)
    # print(f"documents: {documents}")
    return documents


@timing_decorator
def validate_index_data(
    data: List[Dict], params: Dict
) -> Tuple[List[Any], List[Any], List[Dict]]:
    """
    Validates the index data by performing the following steps:
    1. Instantiates a SentenceWindowNodeParser object with the provided parameters.
    2. Retrieves the nodes to validate using the SentenceWindowNodeParser object.
    3. Checks each node for word count outliers and categorizes them accordingly.
    4. Returns the valid nodes, nodes with outliers, and a list of validation descriptives.

    Args:
    - data: The index data to validate.
    - params: A dictionary containing the following parameters:
        - SentenceWindowNodeParser: A dictionary containing the parameters for the SentenceWindowNodeParser object.
            - window_size: The size of the window for the SentenceWindowNodeParser.
            - window_metadata_key: The key to access the window metadata.
            - original_text_metadata_key: The key to access the original text metadata.
            - include_metadata: A boolean indicating whether to include metadata in the nodes.
            - include_prev_next_rel: A boolean indicating whether to include previous and next relationships in the nodes.
        - WORD_COUNT_OUTLIER_CUTOFF: The cutoff value for word count outliers.
        - show_progress: A boolean indicating whether to show progress during node retrieval.

    Returns:
    A tuple containing the following:
    - valid_nodes: A list of nodes that passed the validation.
    - nodes_with_outliers: A list of nodes that exceeded the word count outlier cutoff.
    - validation_descriptives: A list of dictionaries containing validation metrics for each node.
        Each dictionary contains the following:
        - node_id: The ID of the node.
        - node_word_count: The word count of the node's original text.
        - cutoff: The cutoff value for word count outliers.
        - outlier: A boolean indicating whether the node is an outlier.
    """
    sentence_window_parser = SentenceWindowNodeParser(
        window_size=params["SentenceWindowNodeParser"]["window_size"],
        window_metadata_key=params["SentenceWindowNodeParser"]["window_metadata_key"],
        original_text_metadata_key=params["SentenceWindowNodeParser"][
            "original_text_metadata_key"
        ],
        include_metadata=params["SentenceWindowNodeParser"]["include_metadata"],
        include_prev_next_rel=params["SentenceWindowNodeParser"][
            "include_prev_next_rel"
        ],
    )
    nodes_to_validate = sentence_window_parser.get_nodes_from_documents(
        data, show_progress=params["SentenceWindowNodeParser"]["show_progress"]
    )

    validation_descriptives = []
    WORD_COUNT_OUTLIER_CUTOFF = params["WORD_COUNT_OUTLIER_CUTOFF"]
    nodes_with_outliers = []
    valid_nodes = []
    for node in nodes_to_validate:
        num_words_original_text = len(node.metadata["original_text"].split(" "))
        node_metrics = {
            "collection": node.metadata["collection"],
            "doc_id": node.metadata["id"],
            "node_id": node.id_,
            "node_word_count": num_words_original_text,
            "cutoff": WORD_COUNT_OUTLIER_CUTOFF,
        }
        if num_words_original_text > WORD_COUNT_OUTLIER_CUTOFF:
            # print(f"node text exceeds {WORD_COUNT_OUTLIER_CUTOFF} - {node.metadata['id']}:{node.id_}")
            node_metrics["outlier"] = True
            # add index to list here
            nodes_with_outliers.append(node)
        else:
            node_metrics["outlier"] = False
            valid_nodes.append(node)
        validation_descriptives.append(node_metrics)

    filtered_valid_nodes = []
    doc_ids_with_outliers = set([node.metadata["id"] for node in nodes_with_outliers])
    # print(f"doc_ids with outliers: {list(doc_ids_with_outliers)}")
    for node in valid_nodes:
        doc_id = node.metadata["id"]
        if doc_id not in list(doc_ids_with_outliers):
            filtered_valid_nodes.append(node)

    return (filtered_valid_nodes, nodes_with_outliers, validation_descriptives)


@timing_decorator
def compute_validation_metrics(data: Tuple[List[Any], List[Any], List[Dict]]) -> None:
    """
    Compute validation metrics based on the given data.

    Args:
    - data: A tuple containing the following:
        - valid_nodes: A list of valid nodes.
        - nodes_with_outliers: A list of nodes with outliers.
        - validation_descriptives: A list of validation descriptives.

    Returns:
    None
    """
    valid_nodes, invalid_nodes, validation_descriptives = data
    if len(valid_nodes) > 0 and len(invalid_nodes) > 0:

        total_steps = 10
        utc_now = datetime.utcnow()
        utc_now_str = utc_now.strftime("%Y-%m-%dT%H-%M-%S")

        # valid_nodes, nodes_with_outliers, validation_descriptives = data
        with tqdm(total=total_steps, desc="Processing validation metrics") as pbar:
            # Convert validation_descriptives to a DataFrame
            raw_df = pd.DataFrame(validation_descriptives)
            raw_df.to_csv(
                f"data/08_reporting/node_data_{utc_now_str}.csv",
                index=False,
                na_rep="nan",
                encoding="utf-8",
            )
            pbar.update(1)
            descriptives = (
                raw_df.groupby(["doc_id", "outlier", "collection"])["node_word_count"]
                .agg(["sum", "mean", "std", "count"])
                .reset_index()
            )
            # descriptives = descriptives.reindex([0, 1], fill_value=np.nan)

            # Replace False with 'valid_nodes' and fill missing values with appropriate defaults
            descriptives["outlier"] = (
                descriptives["outlier"]
                .replace(False, "valid_nodes")
                .replace(True, "invalid_nodes")
            )
            pbar.update(1)
            # print(descriptives.head())
            unique_doc_ids = raw_df["doc_id"].unique()
            outliers = ["valid_nodes", "invalid_nodes"]
            all_combinations = pd.MultiIndex.from_product(
                [unique_doc_ids, outliers], names=["doc_id", "outlier"]
            )
            all_df = pd.DataFrame(index=all_combinations).reset_index()

            result_df = pd.merge(
                all_df, descriptives, on=["doc_id", "outlier"], how="left"
            )
            result_df = result_df.fillna(
                {"sum": 0, "mean": np.nan, "std": np.nan, "count": 0}
            )
            result_df["outlier"] = result_df["outlier"].astype("category")
            result_df["doc_id"] = result_df["doc_id"].astype("category")

            result_df.set_index(["doc_id", "outlier"], inplace=True)
            result_df = result_df.sort_index()
            pbar.update(1)
            groups = result_df.groupby(level="doc_id")
            new_descriptives = []
            utc_now = datetime.utcnow()
            utc_now_str = utc_now.strftime("%Y-%m-%d %H:%M:%S")
            pbar.update(1)
            for doc_id, group_df in groups:
                # doc_keywords_valid = get_doc_keywords(valid_nodes, doc_id)
                # doc_keywords_invalid = get_doc_keywords(nodes_with_outliers, doc_id)
                # keywords = []
                # if len(doc_keywords_valid):
                #    keywords = doc_keywords_valid
                # elif len(doc_keywords_invalid):
                #    keywords = doc_keywords_invalid
                # num_keywords = len(keywords)

                valid_cols = group_df.loc[(doc_id, "valid_nodes")]
                invalid_cols = group_df.loc[(doc_id, "invalid_nodes")]
                collection = valid_cols["collection"]

                temp_dict = {
                    "batch_id": utc_now_str,
                    "collection": collection,
                    "doc_id": doc_id,
                    "valid_word_sum": valid_cols["sum"],
                    "valid_word_mean": valid_cols["mean"],
                    "valid_word_std": valid_cols["std"],
                    "valid_node_count": valid_cols["count"],
                    "invalid_word_sum": invalid_cols["sum"],
                    "invalid_word_mean": invalid_cols["mean"],
                    "invalid_word_std": invalid_cols["std"],
                    "invalid_node_count": invalid_cols["count"],
                    "total_word_sum": valid_cols["sum"] + invalid_cols["sum"],
                    "total_nodes_count": valid_cols["count"] + invalid_cols["count"],
                }

                new_descriptives.append(temp_dict)
                pbar.update(1)
            # print(f"new_descriptives: {new_descriptives}")
            new_data_df = pd.DataFrame(new_descriptives)
            # Read the current metrics from a CSV file
            current_metrics = pd.read_csv("data/08_reporting/validation_metrics.csv")
            pbar.update(1)
            # Concatenate the current metrics with the new data and update the CSV file
            current_metrics = pd.concat(
                [current_metrics, new_data_df], ignore_index=True
            )
            # print(current_metrics)
            current_metrics.to_csv(
                "data/08_reporting/validation_metrics.csv",
                index=False,
                na_rep="nan",
                encoding="utf-8",
            )
            pbar.update(1)


@timing_decorator
def exclude_links_from_embeddings(documents):
    """
    Exclude links from embeddings for a list of documents.

    Args:
        documents (list): A list of documents.

    Returns:
        list: The modified list of documents with excluded links.
    """
    # documents are filtered
    # extract the links from the metadata and and the links for each
    for doc in documents:
        matching_keys = find_matching_keys(doc)
        # print(f"matching link keys: \n{matching_keys}")
        doc.excluded_embed_metadata_keys.extend(matching_keys)
        # print(f"exlude links docs: {doc.excluded_embed_metadata_keys}\n")
    # print("raw document")
    # print(documents[0])
    return documents


@timing_decorator
def filter_documents(documents, validation_data):
    """
    Filters a list of documents based on a validation data set.

    Args:
        documents (List[Document]): The list of documents to filter.
        validation_data (Tuple[List[int], List[int], Dict[str, float]]): A tuple containing
            the valid document IDs, invalid document IDs, and additional metrics.

    Returns:
        List[Document]: The filtered list of documents.
    """
    filtered_valid_documents = []
    if len(documents) > 0:
        # documents are all the loaded files
        # valid_nodes contains the document ids of the files we can pass through
        valid_nodes, invalid_nodes, metrics = validation_data
        sentence_text_splitter = SentenceSplitter(
            separator=" ",
            chunk_size=512,
            chunk_overlap=0,
            paragraph_separator="\n\n",
            secondary_chunking_regex="[^,.;。]+[,.;。]?",
            tokenizer=tiktoken.encoding_for_model("text-davinci-002").encode,
        )
        valid_doc_ids = set()
        invalid_doc_ids = set()
        # print(f"received {len(valid_nodes)} valid nodes\nreceived {len(invalid_nodes)} invalid nodes")
        for node in valid_nodes:
            valid_doc_ids.add(node.metadata["id"])
        for node in invalid_nodes:
            invalid_doc_ids.add(node.metadata["id"])

        filtered_valid_documents = [
            doc for doc in documents if doc.id_ in list(valid_doc_ids)
        ]
        filtered_invalid_documents = [
            doc for doc in documents if doc.id_ in list(invalid_doc_ids)
        ]
        for doc in tqdm(
            filtered_invalid_documents, desc="Processing documents with text_splitter."
        ):
            doc.text = prepare_for_sentence_parser_llama_sentence(
                doc.text, sentence_text_splitter, 512
            )
            doc.metadata["tags"] = "chunk_warnings"
            doc.excluded_embed_metadata_keys.append("tags")
            doc.excluded_llm_metadata_keys.append("tags")

        for doc in tqdm(filtered_valid_documents, desc="Processing"):
            doc.metadata["tags"] = ""
            doc.excluded_embed_metadata_keys.append("tags")
            doc.excluded_llm_metadata_keys.append("tags")

        filtered_valid_documents.extend(filtered_invalid_documents)

        # save to mongo db + collection
    return filtered_valid_documents


def load_index_documents(data):
    # Incoming: list of Documents
    # Transform: list of dictionaries

    return data


def remove_docstore_source_files(files_to_remove):
    if len(files_to_remove) > 0:

        conf_loader = ConfigLoader(settings.CONF_SOURCE)
        credentials = conf_loader["credentials"]
        azure_blob_credentials = credentials["azure_blob_credentials"]

        account_name = azure_blob_credentials["account_name"]
        account_key = azure_blob_credentials["account_key"]

        # Create an Azure Blob Storage filesystem instance
        fs = filesystem("abfs", account_name=account_name, account_key=account_key)

        # Base path for the files to remove
        path = "abfs://report-data/"

        # Loop through the files and delete each one
        for file in files_to_remove:
            full_path = path + file + ".json"
            if fs.exists(full_path):
                fs.rm(full_path)
                print(f"Deleted file: {full_path}")
            else:
                print(f"File not found: {full_path}")

    return True
