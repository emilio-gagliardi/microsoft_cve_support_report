"""
This is a boilerplate pipeline 'ingest_patchmanagement'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_partitioned_json,
    combine_partitioned_json,
    clean_jsons,
    load_jsons,
    move_jsons_azure,
    add_placeholder,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_partitioned_json,
                inputs="partitioned_cleaned_emails_json",
                outputs="jsons_for_combining",
                name="extract_partitioned_json",
                tags=["daily", "ETL"],
            ),
            node(
                func=combine_partitioned_json,
                inputs="jsons_for_combining",
                outputs="jsons_for_cleaning",
                name="combine_partitioned_json",
                tags=["daily", "ETL"],
            ),
            node(
                func=clean_jsons,
                inputs="jsons_for_cleaning",
                outputs="jsons_for_loading_interm",
                name="clean_jsons",
                tags=["daily", "ETL"],
            ),
            node(
                func=load_jsons,
                inputs="jsons_for_loading_interm",
                outputs="email_jsons_interm_1",
                name="load_jsons",
                tags=["daily", "ETL"],
            ),
            node(
                func=move_jsons_azure,
                inputs=["jsons_for_loading_interm", "params:email_processed_location"],
                outputs=None,
                name="move_jsons_azure",
                tags=["daily", "ETL"],
            ),
            node(
                func=add_placeholder,
                inputs=["params:email_processed_location"],
                outputs=None,
                name="add_placeholder",
                tags=["daily", "ETL"],
            ),
        ]
    )
