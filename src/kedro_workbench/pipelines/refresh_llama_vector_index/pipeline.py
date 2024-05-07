"""
This is a boilerplate pipeline 'refresh_llama_indices'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    load_collection_details,
    load_llama_text_splitter,
    load_llama_node_parser,
    load_vectordb_client,
    load_vectordb_embedding_function,
    load_llm_embed_model,
    load_llama_llm,
    load_vectordb_collections,
    load_vector_stores,
    load_llama_service_context,
    load_vector_storage_contexts,
    load_llama_vector_indices,
    extract_dicts_from_docstore,
    convert_dicts_to_documents,
    convert_list_to_grouped_dict,
    refresh_llama_vector_indices,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_collection_details,
                inputs=["params:collection_names", "params:collection_descriptions"],
                outputs=["collection_names", "collection_descriptions"],
                name="load_collection_details",
            ),
            node(
                func=load_llama_text_splitter,
                inputs=["params:text_splitter"],
                outputs="llama_text_splitter",
                name="load_llama_text_splitter",
            ),
            node(
                func=load_llama_node_parser,
                inputs=["llama_text_splitter", "params:node_parser"],
                outputs="llama_node_parser",
                name="load_llama_node_parser",
            ),
            node(
                func=load_vectordb_client,
                inputs=["params:chroma_store"],
                outputs="vectordb_client",
                name="load_vectordb_client",
            ),
            node(
                func=load_vectordb_embedding_function,
                inputs=["params:chroma_embedding_function"],
                outputs="vectordb_embedding_function",
                name="load_vectordb_embedding_function",
            ),
            node(
                func=load_llm_embed_model,
                inputs=["params:llm_embedding_model"],
                outputs="llm_embedding_model",
                name="load_llm_embed_model",
            ),
            node(
                func=load_llama_llm,
                inputs=["params:llm"],
                outputs="llama_llm",
                name="load_llama_llm",
            ),
            node(
                func=load_vectordb_collections,
                inputs=[
                    "collection_names",
                    "vectordb_client",
                    "vectordb_embedding_function",
                ],
                outputs="vectordb_collections",
                name="load_vectordb_collections",
            ),
            node(
                func=load_vector_stores,
                inputs=["collection_names", "vectordb_collections"],
                outputs="vector_stores",
                name="load_vector_stores",
            ),
            node(
                func=load_llama_service_context,
                inputs=["llama_llm", "llm_embedding_model", "llama_node_parser"],
                outputs="llama_service_context",
                name="load_llama_service_context",
            ),
            node(
                func=load_vector_storage_contexts,
                inputs=["collection_names", "vector_stores"],
                outputs="vector_storage_contexts",
                name="load_vector_storage_contexts",
            ),
            node(
                func=load_llama_vector_indices,
                inputs=["collection_names", "vector_stores", "llama_service_context"],
                outputs="vector_indices_to_refresh",
                name="load_llama_vector_indices",
            ),
            node(
                func=extract_dicts_from_docstore,
                inputs=["params:source_docstore"],
                outputs="new_docs_to_prepare",
                name="extract_dicts_from_docstore",
            ),
            node(
                func=convert_dicts_to_documents,
                inputs=["new_docs_to_prepare"],
                outputs="docs_to_group",
                name="convert_dicts_to_documents",
            ),
            node(
                func=convert_list_to_grouped_dict,
                inputs="docs_to_group",
                outputs="grouped_documents_for_indices",
                name="convert_list_to_grouped_dict",
            ),
            node(
                func=refresh_llama_vector_indices,
                inputs=[
                    "grouped_documents_for_indices",
                    "collection_names",
                    "vector_indices_to_refresh",
                    "vector_storage_contexts",
                    "llama_service_context",
                    "params:source_docstore",
                ],
                outputs=None,
                name="refresh_llama_vector_indices",
            ),
        ]
    )
