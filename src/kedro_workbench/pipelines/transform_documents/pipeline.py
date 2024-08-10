"""
This is a boilerplate pipeline 'transform_documents'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_rss_1_docs,
    transform_rss_1,
    load_transformed_rss_1,
    extract_rss_2_docs,
    transform_rss_2,
    load_transformed_rss_2,
    extract_rss_3_docs,
    transform_rss_3,
    load_transformed_rss_3,
    extract_rss_4_docs,
    transform_rss_4,
    load_transformed_rss_4,
    extract_edge_1_docs,
    transform_edge_1,
    load_transformed_edge_1,
    extract_edge_2_docs,
    transform_edge_2,
    load_transformed_edge_2,
    extract_edge_3_docs,
    transform_edge_3,
    load_transformed_edge_3,
    extract_edge_4_docs,
    transform_edge_4,
    load_transformed_edge_4,
    extract_edge_5_docs,
    transform_edge_5,
    load_transformed_edge_5,
    extract_patchmanagement_docs,
    transform_patchmanagement_docs,
    load_patchmanagement_docs,
)


# add to extract_rss_1_docs
# , "transform_docs_validator_flag"
def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_rss_1_docs,
                inputs=[
                    "rss_1_augmented",
                ],
                outputs="rss_1_data_for_transforming",
                name="extract_rss_1_docs",
                tags=["msrc_security_update"],
            ),
            node(
                func=transform_rss_1,
                inputs=["rss_1_data_for_transforming", "params:rss_1_keys"],
                outputs="transformed_rss_1_for_loading",
                name="transform_rss_1",
                tags=["msrc_security_update"],
            ),
            node(
                func=load_transformed_rss_1,
                inputs=["transformed_rss_1_for_loading"],
                outputs="rss_1_primary",
                name="load_transformed_rss_1",
                tags=["msrc_security_update"],
            ),
            node(
                func=extract_rss_2_docs,
                inputs=[
                    "rss_2_augmented",
                ],
                outputs="rss_2_data_for_transforming",
                name="extract_rss_2_docs",
                tags=["windows_update"],
            ),
            node(
                func=transform_rss_2,
                inputs=["rss_2_data_for_transforming", "params:rss_2_keys"],
                outputs="transformed_rss_2_for_loading",
                name="transform_rss_2",
                tags=["windows_update"],
            ),
            node(
                func=load_transformed_rss_2,
                inputs=["transformed_rss_2_for_loading"],
                outputs="rss_2_primary",
                name="load_transformed_rss_2",
                tags=["windows_update"],
            ),
            node(
                func=extract_rss_3_docs,
                inputs=[
                    "rss_3_augmented",
                ],
                outputs="rss_3_data_for_transforming",
                name="extract_rss_3_docs",
                tags=["windows_10"],
            ),
            node(
                func=transform_rss_3,
                inputs=["rss_3_data_for_transforming", "params:rss_3_keys"],
                outputs="transformed_rss_3_for_loading",
                name="transform_rss_3",
                tags=["windows_10"],
            ),
            node(
                func=load_transformed_rss_3,
                inputs=["transformed_rss_3_for_loading"],
                outputs="rss_3_primary",
                name="load_transformed_rss_3",
                tags=["windows_10"],
            ),
            node(
                func=extract_rss_4_docs,
                inputs=[
                    "rss_4_augmented",
                ],
                outputs="rss_4_data_for_transforming",
                name="extract_rss_4_docs",
                tags=["windows_11"],
            ),
            node(
                func=transform_rss_4,
                inputs=["rss_4_data_for_transforming", "params:rss_4_keys"],
                outputs="transformed_rss_4_for_loading",
                name="transform_rss_4",
                tags=["windows_11"],
            ),
            node(
                func=load_transformed_rss_4,
                inputs=["transformed_rss_4_for_loading"],
                outputs="rss_4_primary",
                name="load_transformed_rss_4",
                tags=["windows_11"],
            ),
            node(
                func=extract_edge_1_docs,
                inputs=[
                    "edge_release_augmented_1",
                ],
                outputs="edge_1_data_for_transforming",
                name="extract_edge_1_docs",
                tags=["stable_channel_notes"],
            ),
            node(
                func=transform_edge_1,
                inputs=["edge_1_data_for_transforming", "params:edge_1_keys"],
                outputs="transformed_edge_1_for_loading",
                name="transform_edge_1",
                tags=["stable_channel_notes"],
            ),
            node(
                func=load_transformed_edge_1,
                inputs=["transformed_edge_1_for_loading"],
                outputs="edge_primary_1",
                name="load_transformed_edge_1",
                tags=["stable_channel_notes"],
            ),
            node(
                func=extract_edge_2_docs,
                inputs=[
                    "edge_release_augmented_2",
                ],
                outputs="edge_2_data_for_transforming",
                name="extract_edge_2_docs",
                tags=["beta_channel_notes"],
            ),
            node(
                func=transform_edge_2,
                inputs=["edge_2_data_for_transforming", "params:edge_2_keys"],
                outputs="transformed_edge_2_for_loading",
                name="transform_edge_2",
                tags=["beta_channel_notes"],
            ),
            node(
                func=load_transformed_edge_2,
                inputs=["transformed_edge_2_for_loading"],
                outputs="edge_primary_2",
                name="load_transformed_edge_2",
                tags=["beta_channel_notes"],
            ),
            node(
                func=extract_edge_3_docs,
                inputs=[
                    "edge_release_augmented_3",
                ],
                outputs="edge_3_data_for_transforming",
                name="extract_edge_3_docs",
                tags=["archive_stable_channel_notes"],
            ),
            node(
                func=transform_edge_3,
                inputs=["edge_3_data_for_transforming", "params:edge_3_keys"],
                outputs="transformed_edge_3_for_loading",
                name="transform_edge_3",
                tags=["archive_stable_channel_notes"],
            ),
            node(
                func=load_transformed_edge_3,
                inputs=["transformed_edge_3_for_loading"],
                outputs="edge_primary_3",
                name="load_transformed_edge_3",
                tags=["archive_stable_channel_notes"],
            ),
            node(
                func=extract_edge_4_docs,
                inputs=[
                    "edge_release_augmented_4",
                ],
                outputs="edge_4_data_for_transforming",
                name="extract_edge_4_docs",
                tags=["mobile_stable_channel_notes"],
            ),
            node(
                func=transform_edge_4,
                inputs=["edge_4_data_for_transforming", "params:edge_4_keys"],
                outputs="transformed_edge_4_for_loading",
                name="transform_edge_4",
                tags=["mobile_stable_channel_notes"],
            ),
            node(
                func=load_transformed_edge_4,
                inputs=["transformed_edge_4_for_loading"],
                outputs="edge_primary_4",
                name="load_transformed_edge_4",
                tags=["mobile_stable_channel_notes"],
            ),
            node(
                func=extract_edge_5_docs,
                inputs=[
                    "edge_release_augmented_5",
                ],
                outputs="edge_5_data_for_transforming",
                name="extract_edge_5_docs",
                tags=["security_update_notes"],
            ),
            node(
                func=transform_edge_5,
                inputs=["edge_5_data_for_transforming", "params:edge_5_keys"],
                outputs="transformed_edge_5_for_loading",
                name="transform_edge_5",
                tags=["security_update_notes"],
            ),
            node(
                func=load_transformed_edge_5,
                inputs=["transformed_edge_5_for_loading"],
                outputs="edge_primary_5",
                name="load_transformed_edge_5",
                tags=["security_update_notes"],
            ),
            node(
                func=extract_patchmanagement_docs,
                inputs=[
                    "email_jsons_augmented_1",
                ],
                outputs="email_jsons_for_transforming",
                name="extract_patchmanagement_docs",
                tags=["patch management"],
            ),
            node(
                func=transform_patchmanagement_docs,
                inputs=["email_jsons_for_transforming", "params:patchmanagement_keys"],
                outputs="transformed_email_jsons_for_loading",
                name="transform_patchmanagement_docs",
                tags=["patch management"],
            ),
            node(
                func=load_patchmanagement_docs,
                inputs=["transformed_email_jsons_for_loading"],
                outputs=["patchmanagement_primary", "transforming_complete"],
                name="load_patchmanagement_docs",
                tags=["patch management"],
            ),
        ],
        tags=["prep for LLM"],
    )
