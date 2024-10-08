"""
This is a boilerplate pipeline 'ingest_rss_data'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_rss_1_feed,
    transform_rss_1_feed,
    load_rss_1_feed,
    extract_rss_2_feed,
    transform_rss_2_feed,
    load_rss_2_feed,
    extract_rss_3_feed,
    transform_rss_3_feed,
    load_rss_3_feed,
    extract_rss_4_feed,
    transform_rss_4_feed,
    load_rss_4_feed,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_rss_1_feed,
                inputs="rss_1_feed_extract",
                outputs="rss_1_feed_for_transforming",
                name="extract_rss_1_feed",
            ),
            node(
                func=transform_rss_1_feed,
                inputs=[
                    "rss_1_feed_for_transforming",
                    "params:rss_1",
                    "params:skip_download",
                ],
                outputs="rss_1_feed_for_loading",
                name="transform_rss_1_feed",
            ),
            node(
                func=load_rss_1_feed,
                inputs="rss_1_feed_for_loading",
                outputs="rss_1_intermediate",
                name="load_rss_1_feed",
            ),
            node(
                func=extract_rss_2_feed,
                inputs="rss_2_feed_extract",
                outputs="rss_2_feed_for_transforming",
                name="extract_rss_2_feed",
            ),
            node(
                func=transform_rss_2_feed,
                inputs=["rss_2_feed_for_transforming", "params:rss_2"],
                outputs="rss_2_feed_for_loading",
                name="transform_rss_2_feed",
            ),
            node(
                func=load_rss_2_feed,
                inputs="rss_2_feed_for_loading",
                outputs="rss_2_intermediate",
                name="load_rss_2_feed",
            ),
            node(
                func=extract_rss_3_feed,
                inputs="rss_3_feed_extract",
                outputs="rss_3_feed_for_transforming",
                name="extract_rss_3_feed",
            ),
            node(
                func=transform_rss_3_feed,
                inputs=["rss_3_feed_for_transforming", "params:rss_3"],
                outputs="rss_3_feed_for_loading",
                name="transform_rss_3_feed",
            ),
            node(
                func=load_rss_3_feed,
                inputs="rss_3_feed_for_loading",
                outputs="rss_3_intermediate",
                name="load_rss_3_feed",
            ),
            node(
                func=extract_rss_4_feed,
                inputs="rss_4_feed_extract",
                outputs="rss_4_feed_for_transforming",
                name="extract_rss_4_feed",
            ),
            node(
                func=transform_rss_4_feed,
                inputs=["rss_4_feed_for_transforming", "params:rss_4"],
                outputs="rss_4_feed_for_loading",
                name="transform_rss_4_feed",
            ),
            node(
                func=load_rss_4_feed,
                inputs="rss_4_feed_for_loading",
                outputs="rss_4_intermediate",
                name="load_rss_4_feed",
            ),
        ]
    )
