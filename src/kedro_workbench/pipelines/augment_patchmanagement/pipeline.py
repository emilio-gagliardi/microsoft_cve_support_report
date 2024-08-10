"""
This is a boilerplate pipeline 'augment_patchmanagement'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_jsons_interm,
    sort_jsons_interm,
    augment_jsons,
    load_jsons_augmented,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_jsons_interm,
                inputs=[
                    "email_jsons_interm_1",
                ],
                outputs="jsons_for_sorting",
                name="extract_jsons_interm",
            ),
            node(
                func=sort_jsons_interm,
                inputs="jsons_for_sorting",
                outputs="jsons_for_augmenting",
                name="sort_jsons_interm",
            ),
            node(
                func=augment_jsons,
                inputs=["jsons_for_augmenting", "params:augment_params"],
                outputs="jsons_for_loading_aug",
                name="augment_jsons",
            ),
            node(
                func=load_jsons_augmented,
                inputs="jsons_for_loading_aug",
                outputs="email_jsons_augmented_1",
                name="load_jsons_augmented",
            ),
        ]
    )
