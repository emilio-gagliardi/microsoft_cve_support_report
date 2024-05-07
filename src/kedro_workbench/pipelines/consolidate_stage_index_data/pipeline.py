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
    begin_preprocessing_pipeline_connector
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
                outputs=["partitioned_index_source_1","load_1_complete"],
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
                outputs=["partitioned_index_source_2","load_2_complete"],
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
                outputs=["partitioned_index_source_3","load_3_complete"],
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
                outputs=["partitioned_index_source_4","load_4_complete"],
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
                outputs=["partitioned_index_source_5","load_5_complete"],
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
                outputs=["partitioned_index_source_6","load_6_complete"],
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
                outputs=["partitioned_index_source_7","load_7_complete"],
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
                outputs=["partitioned_index_source_8","load_8_complete"],
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
                outputs=["partitioned_index_source_9","load_9_complete"],
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
                outputs=["partitioned_index_source_10","load_10_complete"],
                name="load_patch_index",
            ),
             node(
                func=begin_preprocessing_pipeline_connector,
                inputs=["load_1_complete","load_2_complete","load_3_complete","load_4_complete","load_5_complete","load_6_complete","load_7_complete","load_8_complete","load_9_complete","load_10_complete"],
                outputs="source_loading_complete",
                name="begin_preprocessing_pipeline_connector",
                tags=["Source Loading Complete"],
            ),
        ]
    )
