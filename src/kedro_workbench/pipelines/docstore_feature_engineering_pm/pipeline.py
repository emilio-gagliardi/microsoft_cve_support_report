"""
This is a boilerplate pipeline 'docstore_feature_engineering_pm'
generated using Kedro 0.18.11
"""


from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (check_for_classification_msrc_complete, extract_patch_managment_to_clean, transform_feature_engineering_data_patch, clean_email_text, get_unique_token_count,generate_noun_phrases, build_lemmatized_tokens, generate_keywords, filter_keywords, evaluate_keywords_node, evaluate_noun_chunks_node, build_user_prompt_data_patch, fit_classification_prompt_patch, classify_emails_node, batch_update_new_features_patch, remove_mongo_duplicates_patch)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=check_for_classification_msrc_complete,
                inputs="proceed_with_classification_patch",
                outputs="begin_docstore_feature_engineering_pm",
                name="check_for_classification_msrc_complete",
            ),
            node(
                func=extract_patch_managment_to_clean,
                inputs=["params:document_limit", "begin_docstore_feature_engineering_pm"],
                outputs="docstore_patch_data_to_transform",
                name="extract_patch_managment_to_clean",
            ),
            node(
                func=transform_feature_engineering_data_patch,
                inputs=["docstore_patch_data_to_transform","params:mongo_metadata_keys_patch"],
                outputs="docstore_patch_data_to_clean",
                name="transform_feature_engineering_data_patch",
            ),
            node(
                func=clean_email_text,
                inputs="docstore_patch_data_to_clean",
                outputs="cleaned_email_text",
                name="clean_email_text",
            ),
            node(
                func=get_unique_token_count,
                inputs="cleaned_email_text",
                outputs="emails_with_unique_token_count",
                name="get_unique_token_count",
            ),
            node(
                func=generate_noun_phrases,
                inputs="emails_with_unique_token_count",
                outputs="emails_with_noun_phrases",
                name="generate_noun_phrases",
            ),
            node(
                func=build_lemmatized_tokens,
                inputs="emails_with_noun_phrases",
                outputs="emails_with_lemmatized_tokens",
                name="build_lemmatized_tokens",
            ),
            node(
                func=generate_keywords,
                inputs="emails_with_lemmatized_tokens",
                outputs="keywords_generated_data_patch",
                name="generate_keywords",
            ),
            node(
                func=filter_keywords,
                inputs="keywords_generated_data_patch",
                outputs="keywords_filtered_data_intermediate_patch",
                name="filter_keywords",
            ),
            node(
                func=evaluate_keywords_node,
                inputs=["params:patch_evaluation_models.keywords", "keywords_filtered_data_intermediate_patch","params:max_llm_output_tokens_classification_patch","params:llm_temperature"],
                outputs="keywords_evaluated_data_intermediate_patch",
                name="evaluate_keywords_node",
            ),
            node(
                func=evaluate_noun_chunks_node,
                inputs=["params:patch_evaluation_models.noun_chunks", "keywords_evaluated_data_intermediate_patch","params:max_llm_output_tokens_classification_patch","params:llm_temperature"],
                outputs="noun_chunks_data_intermediate_patch",
                name="evaluate_noun_chunks_node",
            ),
            node(
                func=build_user_prompt_data_patch,
                inputs=["noun_chunks_data_intermediate_patch", "params:user_prompt_metadata_keys_patch"],
                outputs="emails_with_prompts",
                name="build_user_prompt_data_patch",
            ),
            node(
                func=fit_classification_prompt_patch,
                inputs=["emails_with_prompts","params:max_prompt_tokens_classification_patch"],
                outputs="emails_with_fit_prompts",
                name="fit_classification_prompt_patch",
            ),
            node(
                func=classify_emails_node,
                inputs=["params:patch_classification_models.custom", "emails_with_fit_prompts","params:max_prompt_tokens_classification_patch", "params:llm_temperature"],
                outputs="feature_engineering_data_patch",
                name="classify_emails_node",
            ),
            node(
                func=batch_update_new_features_patch,
                inputs="feature_engineering_data_patch",
                outputs="batch_fe_update_flag_patch",
                name="batch_update_new_features_patch",
            ),
            node(
                func=remove_mongo_duplicates_patch,
                inputs="batch_fe_update_flag_patch",
                outputs=None,
                name="remove_mongo_duplicates_patch",
            ),
        ]
    )
