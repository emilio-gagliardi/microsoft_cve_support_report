"""
This is a boilerplate pipeline 'preprocess_docs_llama_index'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    get_partitioned_index_data,
    combine_callable_index_data,
    create_documents_from_index_data,
    validate_index_data,
    compute_validation_metrics,
    filter_documents,
    exclude_links_from_embeddings,
    load_index_documents,
    remove_docstore_source_files,
    check_for_loading_complete,
    begin_ingest_proudct_build_pipeline_connector
)

# , "proceed_with_preprocessing"
# get_partitioned_index_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=check_for_loading_complete,
                inputs="source_loading_complete",
                outputs="proceed_with_preprocessing",
                name="check_for_loading_complete",
            ),
            node(
                func=get_partitioned_index_data,
                inputs=[
                    "get_partioned_index_source",
                    "proceed_with_preprocessing"
                    ],
                outputs="index_data_as_callable",
                name="get_partitioned_index_data",
            ),
            node(
                func=combine_callable_index_data,
                inputs="index_data_as_callable",
                outputs=[
                    "index_data_for_documents",
                    "files_to_remove"
                    ],
                name="combine_callable_index_data",
            ),
            node(
                func=create_documents_from_index_data,
                inputs=["index_data_for_documents"],
                outputs="documents_for_index",
                name="create_documents_from_index_data",
            ),
            node(
                func=validate_index_data,
                inputs=[
                    "documents_for_index",
                    "params:validate_new_nodes"
                    ],
                outputs="data_and_descriptives_tuple",
                name="validate_index_data",
            ),
            node(
                func=compute_validation_metrics,
                inputs=["data_and_descriptives_tuple"],
                outputs=None,
                name="compute_validation_metrics",
            ),
            node(
                func=exclude_links_from_embeddings,
                inputs="documents_for_index",
                outputs="documents_for_filtering",
                name="exclude_links_from_embeddings",
            ),
            node(
                func=filter_documents,
                inputs=[
                    "documents_for_filtering",
                    "data_and_descriptives_tuple"
                    ],
                outputs="all_documents_for_index_pkl",
                name="filter_documents",
            ),
            node(
                func=load_index_documents,
                inputs="all_documents_for_index_pkl",
                outputs="load_index_documents_to_mongo",
                name="load_index_documents",
            ),
            node(
                func=remove_docstore_source_files,
                inputs=["files_to_remove"],
                outputs="preprocessing_complete",
                name="remove_docstore_source_files",
            ),
            node(
                func=begin_ingest_proudct_build_pipeline_connector,
                inputs="preprocessing_complete",
                outputs="begin_product_build_ingestion",
                name="begin_ingest_proudct_build_pipeline_connector",
            ),
        ],
        tags=["preprocess index documents"],
    )
