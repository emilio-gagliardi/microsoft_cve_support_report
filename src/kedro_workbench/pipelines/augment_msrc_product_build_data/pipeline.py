"""
This is a boilerplate pipeline 'augment_msrc_product_build_data'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    check_for_product_build_ingestion_complete,
    extract_existing_cve_docs_to_augment,
    extract_existing_product_build_data,
    merge_product_build_data_with_msrc_docs,
    load_augmented_msrc_posts_product_build_docs,
    extract_update_packages_for_augmenting,
    augment_update_packages_additional_details,
    load_augmented_update_packages_to_db,
    parse_restructure_installation_details,
    begin_feature_engineering_pipeline_connector,
)

# Note: removed dataset , "proceed_with_augmenting_product_build_data" from:
# extract_existing_cve_docs_to_augment
# extract_existing_product_build_data
# extract_update_packages_for_augmenting


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=check_for_product_build_ingestion_complete,
                inputs="begin_augmenting_product_build_data",
                outputs="proceed_with_augmenting_product_build_data",
                name="check_for_product_build_ingestion_complete",
            ),
            node(
                func=extract_existing_cve_docs_to_augment,
                inputs=[
                    "params:day_interval",
                    "proceed_with_augmenting_product_build_data",
                ],
                outputs="existing_cves_to_augment_df",
                name="extract_existing_cve_docs_to_augment",
            ),
            node(
                func=extract_existing_product_build_data,
                inputs=[
                    "params:day_interval",
                    "params:product_build_product_patterns",
                    "params:product_build_augment_params.columns_to_keep",
                    "proceed_with_augmenting_product_build_data",
                ],
                outputs="existing_product_builds_to_augment_df",
                name="extract_existing_product_build_data",
            ),
            node(
                func=merge_product_build_data_with_msrc_docs,
                inputs=[
                    "existing_product_builds_to_augment_df",
                    "existing_cves_to_augment_df",
                    "params:product_build_augment_params.columns_to_keep",
                ],
                outputs="augmented_cves_to_update",
                name="merge_product_build_data_with_msrc_docs",
            ),
            node(
                func=load_augmented_msrc_posts_product_build_docs,
                inputs=["augmented_cves_to_update"],
                outputs="augmented_product_build_load_status",
                name="load_augmented_msrc_posts_product_build_docs",
            ),
            node(
                func=extract_update_packages_for_augmenting,
                inputs=[
                    "params:day_interval",
                    "proceed_with_augmenting_product_build_data",
                ],
                outputs="update_packages_to_augment",
                name="extract_update_packages_for_augmenting",
            ),
            node(
                func=augment_update_packages_additional_details,
                inputs=[
                    "update_packages_to_augment",
                    "params:update_package_install_search_criteria",
                ],
                outputs="augmented_update_packages",
                name="augment_update_packages_additional_details",
            ),
            node(
                func=parse_restructure_installation_details,
                inputs=["augmented_update_packages"],
                outputs="update_packages_with_installation_details",
                name="parse_restructure_installation_details",
            ),
            node(
                func=load_augmented_update_packages_to_db,
                inputs=["update_packages_with_installation_details"],
                outputs="augmented_update_package_details_status",
                name="load_augmented_update_packages_to_db",
            ),
            node(
                func=begin_feature_engineering_pipeline_connector,
                inputs=["augmented_update_package_details_status"],
                outputs="begin_docstore_feature_engineering",
                name="begin_feature_engineering_pipeline_connector",
            ),
        ]
    )
