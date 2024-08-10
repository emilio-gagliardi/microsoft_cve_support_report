"""
This is a boilerplate pipeline 'consolidate_stage_index_data'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_rss_1_primary,
    extract_rss_2_primary,
    extract_rss_3_primary,
    extract_rss_4_primary,
    extract_edge_1_primary,
    extract_edge_2_primary,
    extract_edge_3_primary,
    extract_edge_4_primary,
    extract_edge_5_primary,
    load_rss_1_index,
    load_rss_2_index,
    load_rss_3_index,
    load_rss_4_index,
    load_edge_1_index,
    load_edge_2_index,
    load_edge_3_index,
    load_edge_4_index,
    load_edge_5_index,
    extract_patch_primary,
    load_patch_index,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_rss_1_primary,
                inputs=["rss_1_primary", "params:document_limit"],
                outputs="rss_1_data_for_aggregating",
                name="extract_rss_1_primary",
            ),
            node(
                func=load_rss_1_index,
                inputs=["rss_1_data_for_aggregating", "params:filename_parts_rss_1"],
                outputs="partitioned_index_source_1",
                name="load_rss_1_index",
            ),
            node(
                func=extract_rss_2_primary,
                inputs=["rss_2_primary", "params:document_limit"],
                outputs="rss_2_data_for_aggregating",
                name="extract_rss_2_primary",
            ),
            node(
                func=load_rss_2_index,
                inputs=["rss_2_data_for_aggregating", "params:filename_parts_rss_2"],
                outputs="partitioned_index_source_2",
                name="load_rss_2_index",
            ),
            node(
                func=extract_rss_3_primary,
                inputs=["rss_3_primary", "params:document_limit"],
                outputs="rss_3_data_for_aggregating",
                name="extract_rss_3_primary",
            ),
            node(
                func=load_rss_3_index,
                inputs=["rss_3_data_for_aggregating", "params:filename_parts_rss_3"],
                outputs="partitioned_index_source_3",
                name="load_rss_3_index",
            ),
            node(
                func=extract_rss_4_primary,
                inputs=["rss_4_primary", "params:document_limit"],
                outputs="rss_4_data_for_aggregating",
                name="extract_rss_4_primary",
            ),
            node(
                func=load_rss_4_index,
                inputs=["rss_4_data_for_aggregating", "params:filename_parts_rss_4"],
                outputs="partitioned_index_source_4",
                name="load_rss_4_index",
            ),
            node(
                func=extract_edge_1_primary,
                inputs=["edge_primary_1", "params:document_limit"],
                outputs="edge_1_data_for_aggregating",
                name="extract_edge_1_primary",
            ),
            node(
                func=load_edge_1_index,
                inputs=["edge_1_data_for_aggregating", "params:filename_parts_edge_1"],
                outputs="partitioned_index_source_5",
                name="load_edge_1_index",
            ),
            node(
                func=extract_edge_2_primary,
                inputs=["edge_primary_2", "params:document_limit"],
                outputs="edge_2_data_for_aggregating",
                name="extract_edge_2_primary",
            ),
            node(
                func=load_edge_2_index,
                inputs=["edge_2_data_for_aggregating", "params:filename_parts_edge_2"],
                outputs="partitioned_index_source_6",
                name="load_edge_2_index",
            ),
            node(
                func=extract_edge_3_primary,
                inputs=["edge_primary_3", "params:document_limit"],
                outputs="edge_3_data_for_aggregating",
                name="extract_edge_3_primary",
            ),
            node(
                func=load_edge_3_index,
                inputs=["edge_3_data_for_aggregating", "params:filename_parts_edge_3"],
                outputs="partitioned_index_source_7",
                name="load_edge_3_index",
            ),
            node(
                func=extract_edge_4_primary,
                inputs=["edge_primary_4", "params:document_limit"],
                outputs="edge_4_data_for_aggregating",
                name="extract_edge_4_primary",
            ),
            node(
                func=load_edge_4_index,
                inputs=["edge_4_data_for_aggregating", "params:filename_parts_edge_4"],
                outputs="partitioned_index_source_8",
                name="load_edge_4_index",
            ),
            node(
                func=extract_edge_5_primary,
                inputs=["edge_primary_5", "params:document_limit"],
                outputs="edge_5_data_for_aggregating",
                name="extract_edge_5_primary",
            ),
            node(
                func=load_edge_5_index,
                inputs=["edge_5_data_for_aggregating", "params:filename_parts_edge_5"],
                outputs="partitioned_index_source_9",
                name="load_edge_5_index",
            ),
            node(
                func=extract_patch_primary,
                inputs=["patchmanagement_primary", "params:document_limit"],
                outputs="patch_data_for_aggregating",
                name="extract_patch_primary",
            ),
            node(
                func=load_patch_index,
                inputs=["patch_data_for_aggregating", "params:filename_parts_patch"],
                outputs="partitioned_index_source_10",
                name="load_patch_index",
            ),
        ]
    )
