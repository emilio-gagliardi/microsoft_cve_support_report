"""
This is a boilerplate pipeline 'augment_edge_releases'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_stable_channel_docs,
    augment_stable_channel_docs,
    load_stable_channel_docs,
    extract_beta_channel_docs,
    augment_beta_channel_docs,
    load_beta_channel_docs,
    extract_archive_stable_channel_docs,
    augment_archive_stable_channel_docs,
    load_archive_stable_channel_docs,
    extract_mobile_stable_channel_docs,
    augment_mobile_stable_channel_docs,
    load_mobile_stable_channel_docs,
    extract_security_update_docs,
    augment_security_update_docs,
    load_security_update_docs,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_stable_channel_docs,
                inputs="edge_release_intermediate_1",
                outputs="edge_release_for_augmenting_1",
                name="extract_stable_channel_docs",
            ),
            node(
                func=augment_stable_channel_docs,
                inputs=["edge_release_for_augmenting_1", "params:edge_1_augmented"],
                outputs="edge_release_for_loading_1",
                name="augment_stable_channel_docs",
            ),
            node(
                func=load_stable_channel_docs,
                inputs=["edge_release_for_loading_1"],
                outputs="edge_release_augmented_1",
                name="load_stable_channel_docs",
            ),
            node(
                func=extract_beta_channel_docs,
                inputs="edge_release_intermediate_2",
                outputs="edge_release_for_augmenting_2",
                name="extract_beta_channel_docs",
            ),
            node(
                func=augment_beta_channel_docs,
                inputs=["edge_release_for_augmenting_2", "params:edge_2_augmented"],
                outputs="edge_release_for_loading_2",
                name="augment_beta_channel_docs",
            ),
            node(
                func=load_beta_channel_docs,
                inputs=["edge_release_for_loading_2"],
                outputs="edge_release_augmented_2",
                name="load_beta_channel_docs",
            ),
            node(
                func=extract_archive_stable_channel_docs,
                inputs="edge_release_intermediate_3",
                outputs="edge_release_for_augmenting_3",
                name="extract_archive_stable_channel_docs",
            ),
            node(
                func=augment_archive_stable_channel_docs,
                inputs=["edge_release_for_augmenting_3", "params:edge_3_augmented"],
                outputs="edge_release_for_loading_3",
                name="augment_archive_stable_channel_docs",
            ),
            node(
                func=load_archive_stable_channel_docs,
                inputs=["edge_release_for_loading_3"],
                outputs="edge_release_augmented_3",
                name="load_archive_stable_channel_docs",
            ),
            node(
                func=extract_mobile_stable_channel_docs,
                inputs="edge_release_intermediate_4",
                outputs="edge_release_for_augmenting_4",
                name="extract_mobile_stable_channel_docs",
            ),
            node(
                func=augment_mobile_stable_channel_docs,
                inputs=["edge_release_for_augmenting_4", "params:edge_4_augmented"],
                outputs="edge_release_for_loading_4",
                name="augment_mobile_stable_channel_docs",
            ),
            node(
                func=load_mobile_stable_channel_docs,
                inputs=["edge_release_for_loading_4"],
                outputs="edge_release_augmented_4",
                name="load_mobile_stable_channel_docs",
            ),
            node(
                func=extract_security_update_docs,
                inputs="edge_release_intermediate_5",
                outputs="edge_release_for_augmenting_5",
                name="extract_security_update_docs",
            ),
            node(
                func=augment_security_update_docs,
                inputs=["edge_release_for_augmenting_5", "params:edge_5_augmented"],
                outputs="edge_release_for_loading_5",
                name="augment_security_update_docs",
            ),
            node(
                func=load_security_update_docs,
                inputs=["edge_release_for_loading_5"],
                outputs="edge_release_augmented_5",
                name="load_security_update_docs",
            ),
        ]
    )
