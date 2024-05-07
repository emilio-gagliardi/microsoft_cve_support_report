"""
This is a boilerplate pipeline 'refresh_llama_indices'
generated using Kedro 0.18.11
"""
import os
import time
from llama_index import (
    VectorStoreIndex,
    ServiceContext,
    Document,
    set_global_service_context,
    load_index_from_storage,
)
from llama_index.callbacks import CallbackManager, LlamaDebugHandler
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.llms import OpenAI
from llama_index.node_parser import SimpleNodeParser, SentenceWindowNodeParser
from llama_index.schema import MetadataMode
from llama_index.storage import StorageContext
from llama_index.storage.docstore import SimpleDocumentStore
from llama_index.storage.index_store import SimpleIndexStore
from llama_index.storage.storage_context import StorageContext
from llama_index.text_splitter import TokenTextSplitter, SentenceSplitter
from llama_index.vector_stores import ChromaVectorStore
import tiktoken
import chromadb
from chromadb.utils import embedding_functions
import openai
from icecream import ic

ic.configureOutput(includeContext=True)
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from kedro_workbench.utils.decorators import timing_decorator
from kedro_workbench.utils.llm_utils import (
    restructure_documents_by_collection,
    yield_n_documents,
)
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs

conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
open_ai_credentials = credentials["OPENAI"]
openai.api_key = open_ai_credentials["api_key"]

llama_debug = LlamaDebugHandler(print_trace_on_end=True)
callback_manager = CallbackManager([llama_debug])


def load_collection_details(collection_names_params, collection_descriptions_params):
    collection_names = collection_names_params
    assert isinstance(collection_names, list), "collection_names must be a list"

    collection_descriptions = collection_descriptions_params
    assert isinstance(
        collection_descriptions, dict
    ), "collection_descriptions must be a dict"

    return collection_names, collection_descriptions


def load_vectordb_client(chroma_store_params):

    chroma_client = chromadb.PersistentClient(path=chroma_store_params["persist_dir"])

    return chroma_client


def load_vectordb_embedding_function(embedding_function_params):
    embedding_function = embedding_functions.OpenAIEmbeddingFunction(
        model_name=embedding_function_params["model_name"],
        api_key=openai.api_key,
    )
    return embedding_function


def load_vectordb_collections(
    collection_names, vectordb_client, vectordb_embedding_function
):

    vectordb_collections = {}
    for item in collection_names:
        # print(item)
        vectordb_collections[item] = vectordb_client.get_or_create_collection(
            name=item, embedding_function=vectordb_embedding_function
        )

    # print(f"{len(vectordb_collections.keys())} collections of type {type(vectordb_collections['windows_10'])}")

    return vectordb_collections


def load_vector_stores(collection_names, vectordb_collections):
    # vector_stores = ChromaVectorStore(chroma_collection=chroma_collection)
    vector_stores = {}
    for item in collection_names:
        vector_stores[item] = ChromaVectorStore(
            chroma_collection=vectordb_collections[item]
        )
    print(
        f"{len(vector_stores.keys())} vector_stores of type {type(vector_stores['windows_10'])}"
    )
    return vector_stores


def load_vector_storage_contexts(collection_names, vector_stores):
    vector_store_storage_contexts = {}
    for item in collection_names:
        vector_store_storage_contexts[item] = StorageContext.from_defaults(
            vector_store=vector_stores[item]
        )
    # print(f"{len(vector_store_storage_contexts.keys())} storage_contexts of type {type(vector_store_storage_contexts['windows_10'])}")
    return vector_store_storage_contexts


def load_llama_text_splitter(text_splitter_params):

    if text_splitter_params["default"] == "TokenTextSplitter":

        text_splitter = TokenTextSplitter(
            separator=text_splitter_params["TokenTextSplitter"]["separator"],
            chunk_size=text_splitter_params["TokenTextSplitter"]["chunk_size"],
            chunk_overlap=text_splitter_params["TokenTextSplitter"]["chunk_overlap"],
            backup_separators=text_splitter_params["TokenTextSplitter"][
                "backup_separators"
            ],
            tokenizer=tiktoken.encoding_for_model("gpt-3.5-turbo").encode,
        )
    else:
        # couldn't load tiktoken from yml for some reason
        text_splitter = SentenceSplitter(
            separator=text_splitter_params["SentenceSplitter"]["separator"],
            chunk_size=text_splitter_params["SentenceSplitter"]["chunk_size"],
            chunk_overlap=text_splitter_params["SentenceSplitter"]["chunk_overlap"],
            paragraph_separator=text_splitter_params["SentenceSplitter"][
                "paragraph_separator"
            ],
            secondary_chunking_regex=text_splitter_params["SentenceSplitter"][
                "secondary_chunking_regex"
            ],
            tokenizer=tiktoken.encoding_for_model("gpt-3.5-turbo").encode,
        )

    return text_splitter


def load_llama_node_parser(text_splitter, node_parser_params):
    """
    Creates and returns a `SentenceWindowNodeParser` object based on the given `node_parser_params`.

    Args:
        node_parser_params (dict): A dictionary containing the parameters for the `SentenceWindowNodeParser` object.
            - window_size (int): The size of the window for the `SentenceWindowNodeParser`.
            - window_metadata_key (str): The key to access the window metadata in the `SentenceWindowNodeParser`.
            - original_text_metadata_key (str): The key to access the original text metadata in the `SentenceWindowNodeParser`.
            - include_metadata (bool): Flag to indicate whether to include metadata in the `SentenceWindowNodeParser`.
            - include_prev_next_rel (bool): Flag to indicate whether to include previous and next relations in the `SentenceWindowNodeParser`.

    Returns:
        SentenceWindowNodeParser
    """
    if node_parser_params["default"] == "SentenceWindowNodeParser":

        node_parser = SentenceWindowNodeParser(
            window_size=node_parser_params["SentenceWindowNodeParser"]["window_size"],
            window_metadata_key=node_parser_params["SentenceWindowNodeParser"][
                "window_metadata_key"
            ],
            original_text_metadata_key=node_parser_params["SentenceWindowNodeParser"][
                "original_text_metadata_key"
            ],
            include_metadata=node_parser_params["SentenceWindowNodeParser"][
                "include_metadata"
            ],
            include_prev_next_rel=node_parser_params["SentenceWindowNodeParser"][
                "include_prev_next_rel"
            ],
        )
    else:

        node_parser = SimpleNodeParser(
            text_splitter=text_splitter,
            include_metadata=node_parser_params["SimpleNodeParser"]["include_metadata"],
            include_prev_next_rel=node_parser_params["SimpleNodeParser"][
                "include_prev_next_rel"
            ],
        )
    print(type(node_parser))
    return node_parser


def load_llm_embed_model(embed_model_params):

    embedding_model = OpenAIEmbedding(
        model=embed_model_params["OpenAIEmbedding"]["model"],
        embed_batch_size=embed_model_params["OpenAIEmbedding"]["batch_size"],
    )

    return embedding_model


def load_llama_llm(llm_params):
    """
    Load the Llama LLM model with the given parameters.

    Args:
        llm_params (dict): A dictionary containing the parameters for the Llama LLM model. It should have the following keys:
            - "openai" (dict): A dictionary containing the OpenAI parameters for the Llama LLM model. It should have the following keys:
                - "temperature" (float): The temperature parameter for the OpenAI model.
                - "model" (str): The model parameter for the OpenAI model.
                - "max_tokens" (int): The max_tokens parameter for the OpenAI model.

    Returns:
        llm (OpenAI): The loaded Llama LLM model.
    """

    # Create and return the Llama LLM model
    llm = OpenAI(
        temperature=llm_params["openai"]["temperature"],
        model=llm_params["openai"]["model"],
        max_tokens=llm_params["openai"]["max_tokens"],
    )
    return llm


def load_llama_service_context(llm, embed_model, node_parser):
    """
    Load the Llama service context.

    This function takes in a Llama object (llm) and a node parser object (node_parser) and returns the loaded service context.
    Args:
        llm: The LLM object.
        node_parser: The node parser object.

    Returns: The loaded service context.
    """
    # Create a ServiceContext object with default values
    service_context = ServiceContext.from_defaults(
        callback_manager=callback_manager,
        llm=llm,
        embed_model=embed_model,
        node_parser=node_parser,
    )
    set_global_service_context(service_context)
    return service_context


def load_llama_vector_indices(collection_names, vector_stores, service_context):

    vector_store_indices = {}
    for item in collection_names:
        vector_store_indices[item] = VectorStoreIndex.from_vector_store(
            vector_store=vector_stores[item], service_context=service_context
        )
    return vector_store_indices


""" def persist_llama_vector_index(vector_index):
    vector_index.storage_context.docstore.persist(persist_path="C:/projects/technical-notes-llm-report/data/06_models/chroma_db/docstore.json")
    vector_index.storage_context.index_store.persist(persist_path="C:/projects/technical-notes-llm-report/data/06_models/chroma_db/index_store.json") """


def extract_dicts_from_docstore(docstore_params):
    mongo_credentials = credentials["mongo_atlas"]
    db_client = MongoDBDocs(
        mongo_db=docstore_params["db"],
        mongo_collection=docstore_params["collection"],
        credentials={
            "username": mongo_credentials["username"],
            "password": mongo_credentials["password"],
        },
    )
    # list_of_document_dicts = db_client.find_docs({'metadata.added_to_vector_store': {"$eq":False}})
    db_cursor = db_client.collection.aggregate(
        [
            {"$match": {"metadata.added_to_vector_store": {"$eq": False}}},
            {
                "$project": {
                    "id_": 0,
                    "hash": 0,
                    "metadata.cve_fixes": 0,
                    "metadata.cve_mentions": 0,
                    "metadata.summary_index": 0,
                    "metadata.tags": 0,
                }
            },
        ]
    )
    list_of_dicts = list(db_cursor)
    # ic(list_of_dicts[0])
    ic(f"num docs from docstore: {len(list_of_dicts)}")
    return list_of_dicts


def convert_dicts_to_documents(list_of_dicts):
    list_of_documents = [Document.from_dict(data) for data in list_of_dicts]
    # ic(f"num docs({len(list_of_documents)}) type of {type(list_of_documents[0])}\n{list_of_documents[0]}")
    return list_of_documents


def convert_list_to_grouped_dict(list_of_documents):
    ic("converting list to grouped dict\n")
    grouped_documents = restructure_documents_by_collection(list_of_documents)

    return grouped_documents


def refresh_llama_vector_indices(
    documents_as_dict,
    collection_names,
    vector_indices,
    storage_contexts,
    service_context,
    docstore_params,
):
    ic("refreshing vector indices\n")
    generator = yield_n_documents(documents_as_dict, 400)
    mongo_credentials = credentials["mongo_atlas"]
    db_client = MongoDBDocs(
        mongo_db=docstore_params["db"],
        mongo_collection=docstore_params["collection"],
        credentials={
            "username": mongo_credentials["username"],
            "password": mongo_credentials["password"],
        },
    )
    for batch in generator:
        batch_ids = []
        for item in collection_names:
            if batch.get(item) is not None:
                print(f"processing collection {item} in current batch\n")
                # print("get list of documents")
                docs_for_collection = batch[item]

                # print(f"type of parent {type(docs_for_collection)} \n number of documents {len(docs_for_collection)}")
                if len(docs_for_collection):
                    # process vector_index refresh()
                    results = vector_indices[item].refresh_ref_docs(
                        docs_for_collection,
                        storage_context=storage_contexts[item],
                        service_context=service_context,
                        show_progress=True,
                    )
                    sum_of_trues = sum(1 for value in results if value)
                    total_results = len(results)
                    # average_of_trues = sum_of_trues / total_results
                    print(
                        f"num docs Inserted/Updated into vector_store: {sum_of_trues} fraction of True/False {sum_of_trues}/{total_results}={sum_of_trues/total_results}"
                    )
                    # save_token_tracking_event(token_counter, f"generate_embeddings_vector_store_index_{item}", "OpenAIEmbeddings")
                    batch_ids = [doc.metadata["id"] for doc in docs_for_collection]
                    ic(len(batch_ids))
                    # update doc.metadata['added_to_vector_store']=True
                    filter_criteria = {"metadata.id": {"$in": batch_ids}}
                    update_operation = {
                        "$set": {"metadata.added_to_vector_store": True}
                    }
                    mongo_update_result = db_client.collection.update_many(
                        filter_criteria, update_operation
                    )
                    print(
                        f"mongo records in this batch: {mongo_update_result.matched_count}, mongo records updated this batch:{mongo_update_result.modified_count}\n"
                    )
                    # pprint(docs_for_collection[len(docs_for_collection)-1])
                else:
                    # remove else when deve is complete
                    print("collection is empty this batch")

            else:
                print(f"no data for collection {item}")
        time.sleep(5)
