"""
This is a boilerplate pipeline 'augment_rss_data'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_rss_1_data,
    augment_rss_1_data,
    load_rss_1_augmented,
    extract_rss_2_data,
    augment_rss_2_data,
    load_rss_2_augmented,
    extract_rss_3_data,
    augment_rss_3_data,
    load_rss_3_augmented,
    extract_rss_4_data,
    augment_rss_4_data,
    load_rss_4_augmented,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_rss_1_data,
                inputs="rss_1_intermediate",
                outputs="rss_1_data_for_augmenting",
                name="extract_rss_1_data",
            ),
            node(
                func=augment_rss_1_data,
                inputs=["rss_1_data_for_augmenting", "params:rss_1_augmented"],
                outputs="rss_1_data_for_loading",
                name="augment_rss_1_data",
            ),
            node(
                func=load_rss_1_augmented,
                inputs=["rss_1_data_for_loading"],
                outputs="rss_1_augmented",
                name="load_rss_1_aug",
            ),
            node(
                func=extract_rss_2_data,
                inputs="rss_2_intermediate",
                outputs="rss_2_data_for_augmenting",
                name="extract_rss_2_data",
            ),
            node(
                func=augment_rss_2_data,
                inputs=["rss_2_data_for_augmenting", "params:rss_2_augmented"],
                outputs="rss_2_data_for_loading",
                name="augment_rss_2_data",
            ),
            node(
                func=load_rss_2_augmented,
                inputs=["rss_2_data_for_loading"],
                outputs="rss_2_augmented",
                name="load_rss_2_aug",
            ),
            node(
                func=extract_rss_3_data,
                inputs="rss_3_intermediate",
                outputs="rss_3_data_for_augmenting",
                name="extract_rss_3_data",
            ),
            node(
                func=augment_rss_3_data,
                inputs=["rss_3_data_for_augmenting", "params:rss_3_augmented"],
                outputs="rss_3_data_for_loading",
                name="augment_rss_3_data",
            ),
            node(
                func=load_rss_3_augmented,
                inputs=["rss_3_data_for_loading"],
                outputs="rss_3_augmented",
                name="load_rss_3_aug",
            ),
            node(
                func=extract_rss_4_data,
                inputs="rss_4_intermediate",
                outputs="rss_4_data_for_augmenting",
                name="extract_rss_4_data",
            ),
            node(
                func=augment_rss_4_data,
                inputs=["rss_4_data_for_augmenting", "params:rss_4_augmented"],
                outputs="rss_4_data_for_loading",
                name="augment_rss_4_data",
            ),
            node(
                func=load_rss_4_augmented,
                inputs=["rss_4_data_for_loading"],
                outputs="rss_4_augmented",
                name="load_rss_4_aug",
            ),
        ]
    )
