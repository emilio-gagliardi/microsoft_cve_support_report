"""
This is a boilerplate pipeline 'ingest_edge_releases'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_edge_release_1,
    load_edge_release_1,
    extract_edge_release_2,
    load_edge_release_2,
    extract_edge_release_3,
    load_edge_release_3,
    extract_edge_release_4,
    load_edge_release_4,
    extract_edge_release_5,
    load_edge_release_5,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_edge_release_1,
                inputs=["html_1_extract"],
                outputs="html_for_loading_1",
                name="extract_edge_release_1",
            ),
            node(
                func=load_edge_release_1,
                inputs="html_for_loading_1",
                outputs="edge_release_intermediate_1",
                name="load_edge_release_1",
            ),
            node(
                func=extract_edge_release_2,
                inputs=["html_2_extract"],
                outputs="html_for_loading_2",
                name="extract_edge_release_2",
            ),
            node(
                func=load_edge_release_2,
                inputs="html_for_loading_2",
                outputs="edge_release_intermediate_2",
                name="load_edge_release_2",
            ),
            node(
                func=extract_edge_release_3,
                inputs=["html_3_extract"],
                outputs="html_for_loading_3",
                name="extract_edge_release_3",
            ),
            node(
                func=load_edge_release_3,
                inputs="html_for_loading_3",
                outputs="edge_release_intermediate_3",
                name="load_edge_release_3",
            ),
            node(
                func=extract_edge_release_4,
                inputs=["html_4_extract"],
                outputs="html_for_loading_4",
                name="extract_edge_release_4",
            ),
            node(
                func=load_edge_release_4,
                inputs="html_for_loading_4",
                outputs="edge_release_intermediate_4",
                name="load_edge_release_4",
            ),
            node(
                func=extract_edge_release_5,
                inputs=["html_5_extract"],
                outputs="html_for_loading_5",
                name="extract_edge_release_5",
            ),
            node(
                func=load_edge_release_5,
                inputs="html_for_loading_5",
                outputs="edge_release_intermediate_5",
                name="load_edge_release_5",
            ),
        ]
    )
