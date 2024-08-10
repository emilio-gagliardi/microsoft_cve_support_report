"""
This is a boilerplate pipeline 'docstore_feature_engineering_posts'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    extract_posts_to_classify,
    transform_classification_data_msrc,
    build_user_prompt_data_msrc,
    fit_classification_prompt_msrc,
    classify_post_msrc_node,
    batch_update_post_types_msrc,
    remove_mongo_duplicates_msrc,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=extract_posts_to_classify,
                inputs=[
                    "params:document_limit",
                ],
                outputs="docstore_to_classify_msrc",
                name="extract_posts_to_classify",
            ),
            node(
                func=transform_classification_data_msrc,
                inputs=[
                    "docstore_to_classify_msrc",
                    "params:user_prompt_metadata_keys_msrc",
                ],
                outputs="docs_to_classify_msrc_df",
                name="transform_classification_data_msrc",
            ),
            node(
                func=build_user_prompt_data_msrc,
                inputs=[
                    "docs_to_classify_msrc_df",
                    "params:user_prompt_metadata_keys_msrc",
                ],
                outputs="docs_with_prompt_strings_msrc",
                name="build_user_prompt_data_msrc",
            ),
            node(
                func=fit_classification_prompt_msrc,
                inputs=[
                    "docs_with_prompt_strings_msrc",
                    "params:max_prompt_tokens_classification_msrc",
                ],
                outputs="docs_with_fit_prompts_msrc",
                name="fit_classification_prompt_msrc",
            ),
            node(
                func=classify_post_msrc_node,
                inputs=[
                    "params:msrc_classification_models.gpt4",
                    "docs_with_fit_prompts_msrc",
                    "params:max_llm_output_tokens_classification_msrc",
                    "params:llm_temperature",
                ],
                outputs="classification_data_msrc",
                name="classify_post_msrc_node",
            ),
            node(
                func=batch_update_post_types_msrc,
                inputs=["classification_data_msrc"],
                outputs="batch_classification_update_flag_msrc",
                name="batch_update_post_types_msrc",
            ),
            node(
                func=remove_mongo_duplicates_msrc,
                inputs=["batch_classification_update_flag_msrc"],
                outputs="classification_msrc_status",
                name="remove_mongo_duplicates_msrc",
            ),
        ]
    )
