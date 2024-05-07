"""
This is a boilerplate pipeline 'ingest_product_build_cve_data'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (download_edge_product_build_data, download_windows10_product_build_data, download_windows11_product_build_data, extract_product_build_cve_windows_10_node, extract_product_build_cve_windows_11_node, extract_product_build_cve_edge_node, preprocess_update_guide_product_build_data_windows10, preprocess_update_guide_product_build_data_windows11, preprocess_update_guide_product_build_data_edge, compile_update_guide_products_from_build_data_windows10, compile_update_guide_products_from_build_data_windows11, compile_update_guide_products_from_build_data_edge,compile_update_guide_build_data_windows10_node, compile_update_guide_build_data_windows11_node, compile_update_guide_build_data_edge_node, compile_update_guide_update_packages_windows10_node, compile_update_guide_update_packages_windows11_node, compile_update_guide_update_packages_edge_node, compile_update_guide_kb_article_data_windows10, compile_update_guide_kb_article_data_windows11, compile_update_guide_kb_article_data_edge, concatenate_update_guide_product_build_data, concatenate_update_guide_products_data, concatenate_update_guide_update_packages_data, concatenate_update_guide_kb_article_data, transform_update_guide_product_build_data_to_list, transform_update_guide_product_data_to_list, transform_update_guide_update_package_data_to_list, transform_update_guide_kb_articles_to_list, load_update_guide_product_build_data, load_update_guide_product_data, load_update_guide_update_package_data, load_update_guide_kb_article_data, check_for_preprocessing_complete, begin_augment_proudct_build_pipeline_connector)

# remove flag from node inputs to run in isolation:
# , "proceed_with_product_build_ingestion"
# download_windows10_product_build_data
# download_windows11_product_build_data
# download_edge_product_build_data

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        
        node(
            func=check_for_preprocessing_complete,
            inputs=["begin_product_build_ingestion"],
            outputs="proceed_with_product_build_ingestion",
            name="check_for_preprocessing_complete",
            ),
        node(
            func=download_windows10_product_build_data,
            inputs=["params:skip_download", "params:product_build_ingestion_params", "params:scrape_build_data_headless", "proceed_with_product_build_ingestion"],
            outputs="windows_10_download_node_status",
            name="download_windows10_product_build_data",
            ),
        node(
            func=download_windows11_product_build_data,
            inputs=["params:skip_download", "params:product_build_ingestion_params", "params:scrape_build_data_headless", "proceed_with_product_build_ingestion"],
            outputs="windows_11_download_node_status",
            name="download_windows11_product_build_data",
            ),
        node(
            func=download_edge_product_build_data,
            inputs=["params:skip_download", "params:product_build_ingestion_params", "params:scrape_build_data_headless", "proceed_with_product_build_ingestion"],
            outputs="edge_download_node_status",
            name="download_edge_product_build_data",
            ),
        node(
            func=extract_product_build_cve_windows_10_node,
            inputs=["windows_10_download_node_status", "extract_product_build_cve_windows_10", "params:product_build_product_patterns.windows_10", "params:overwrite"],
            outputs="raw_product_build_cve_windows_10_df",
            name="extract_product_build_cve_windows_10_node",
            ),
        node(
            func=extract_product_build_cve_windows_11_node,
            inputs=["windows_11_download_node_status", "extract_product_build_cve_windows_11", "params:product_build_product_patterns.windows_11", "params:overwrite"],
            outputs="raw_product_build_cve_windows_11_df",
            name="extract_product_build_cve_windows_11_node",
            ),
        node(
            func=extract_product_build_cve_edge_node,
            inputs=["edge_download_node_status", "extract_product_build_cve_edge", "params:overwrite"],
            outputs="raw_product_build_cve_edge_df",
            name="extract_product_build_cve_edge_node",
            ),
        node(
            func=preprocess_update_guide_product_build_data_windows10,
            inputs=["params:product_build_preprocess_params.windows10", "raw_product_build_cve_windows_10_df"],
            outputs="prepped_product_build_cve_windows_10_df",
            name="preprocess_update_guide_product_build_data_windows10",
            ),
        node(
            func=preprocess_update_guide_product_build_data_windows11,
            inputs=["params:product_build_preprocess_params.windows11", "raw_product_build_cve_windows_11_df"],
            outputs="prepped_product_build_cve_windows_11_df",
            name="preprocess_update_guide_product_build_data_windows11",
            ),
        node(
            func=preprocess_update_guide_product_build_data_edge,
            inputs=["params:product_build_preprocess_params.edge", "raw_product_build_cve_edge_df"],
            outputs="prepped_product_build_cve_edge_df",
            name="preprocess_update_guide_product_build_data_edge",
            ),
        node(
            func=compile_update_guide_products_from_build_data_windows10,
            inputs=["params:compile_products_params.windows10", "prepped_product_build_cve_windows_10_df"],
            outputs="products_windows10_df",
            name="compile_update_guide_products_from_build_data_windows10",
            ),
        node(
            func=compile_update_guide_products_from_build_data_windows11,
            inputs=["params:compile_products_params.windows11", "prepped_product_build_cve_windows_11_df"],
            outputs="products_windows11_df",
            name="compile_update_guide_products_from_build_data_windows11",
            ),
        node(
            func=compile_update_guide_products_from_build_data_edge,
            inputs=["params:compile_products_params.edge", "prepped_product_build_cve_edge_df"],
            outputs="products_edge_df",
            name="compile_update_guide_products_from_build_data_edge",
            ),
        node(
            func=compile_update_guide_kb_article_data_windows10,
            inputs=["params:compile_kb_articles_params.windows10", "prepped_product_build_cve_windows_10_df"],
            outputs="kb_articles_windows10_df",
            name="compile_update_guide_kb_article_data_windows10",
            ),
        node(
            func=compile_update_guide_kb_article_data_windows11,
            inputs=["params:compile_kb_articles_params.windows11", "prepped_product_build_cve_windows_11_df"],
            outputs="kb_articles_windows11_df",
            name="compile_update_guide_kb_article_data_windows11",
            ),
        node(
            func=compile_update_guide_kb_article_data_edge,
            inputs=["params:compile_kb_articles_params.edge", "prepped_product_build_cve_edge_df"],
            outputs="kb_articles_edge_df",
            name="compile_update_guide_kb_article_data_edge",
            ),
        node(
            func=compile_update_guide_build_data_windows10_node,
            inputs=["params:compile_product_build_params.windows10", "prepped_product_build_cve_windows_10_df"],
            outputs="product_builds_windows10_df",
            name="compile_update_guide_build_data_windows10_node",
            ),
        node(
            func=compile_update_guide_build_data_windows11_node,
            inputs=["params:compile_product_build_params.windows11", "prepped_product_build_cve_windows_11_df"],
            outputs="product_builds_windows11_df",
            name="compile_update_guide_build_data_windows11_node",
            ),
        node(
            func=compile_update_guide_build_data_edge_node,
            inputs=["params:compile_product_build_params.edge", "prepped_product_build_cve_edge_df"],
            outputs="product_builds_edge_df",
            name="compile_update_guide_build_data_edge_node",
            ),
        node(
            func=compile_update_guide_update_packages_windows10_node,
            inputs=["params:compile_update_package_params.windows10", "prepped_product_build_cve_windows_10_df"],
            outputs="update_packages_windows_10_df",
            name="compile_update_guide_update_packages_windows10_node",
            ),
        node(
            func=compile_update_guide_update_packages_windows11_node,
            inputs=["params:compile_update_package_params.windows11", "prepped_product_build_cve_windows_11_df"],
            outputs="update_packages_windows_11_df",
            name="compile_update_guide_update_packages_windows11_node",
            ),
        node(
            func=compile_update_guide_update_packages_edge_node,
            inputs=["params:compile_update_package_params.edge", "prepped_product_build_cve_edge_df"],
            outputs="update_packages_edge_df",
            name="compile_update_guide_update_packages_edge_node",
            ),
        node(
            func=concatenate_update_guide_product_build_data,
            inputs=["params:concatenate_product_build_params", "product_builds_windows10_df", "product_builds_windows11_df", "product_builds_edge_df"],
            outputs="concatenated_product_build_df",
            name="concatenate_update_guide_product_build_data",
            ),
        node(
            func=concatenate_update_guide_products_data,
            inputs=["params:concatenate_products_params","products_windows10_df", "products_windows11_df", "products_edge_df"],
            outputs="concatenated_products_df",
            name="concatenate_update_guide_products_data",
            ),
        node(
            func=concatenate_update_guide_kb_article_data,
            inputs=["params:concatenate_kb_article_params","kb_articles_windows10_df", "kb_articles_windows11_df", "kb_articles_edge_df"],
            outputs="concatenated_kb_articles_df",
            name="concatenate_update_guide_kb_article_data",
            ),
        node(
            func=concatenate_update_guide_update_packages_data,
            inputs=["params:concatenate_update_package_params","update_packages_windows_10_df", "update_packages_windows_11_df", "update_packages_edge_df"],
            outputs="concatenated_update_packages_df",
            name="concatenate_update_guide_update_packages_data",
            ),
        node(
            func=transform_update_guide_product_data_to_list,
            inputs=["concatenated_products_df"],
            outputs="transformed_products_for_loading",
            name="transform_update_guide_product_data_to_list",
            ),
        node(
            func=transform_update_guide_product_build_data_to_list,
            inputs=["concatenated_product_build_df"],
            outputs=["transformed_product_builds_for_loading", "wait_for_transform"],
            name="transform_update_guide_product_build_data_to_list",
            ),
        node(
            func=transform_update_guide_kb_articles_to_list,
            inputs=["concatenated_kb_articles_df"],
            outputs="transformed_kb_articles_for_loading",
            name="transform_update_guide_kb_articles_to_list",
            ),
        node(
            func=transform_update_guide_update_package_data_to_list,
            inputs=["concatenated_update_packages_df"],
            outputs="transformed_update_packages_for_loading",
            name="transform_update_guide_update_package_data_to_list",
            ),
        node(
            func=load_update_guide_product_build_data,
            inputs=["transformed_product_builds_for_loading", "params:overwrite"],
            outputs=None,
            name="load_update_guide_product_build_data",
            ),
        node(
            func=load_update_guide_product_data,
            inputs=["transformed_products_for_loading"],
            outputs=None,
            name="load_update_guide_product_data",
            ),
        node(
            func=load_update_guide_kb_article_data,
            inputs=["transformed_kb_articles_for_loading", "params:overwrite"],
            outputs=None,
            name="load_update_guide_kb_article_data",
            ),
        node(
            func=load_update_guide_update_package_data,
            inputs=["transformed_update_packages_for_loading", "params:overwrite"],
            outputs="product_build_ingestion_complete",
            name="load_update_guide_update_package_data",
            ),
        node(
            func=begin_augment_proudct_build_pipeline_connector,
            inputs=["product_build_ingestion_complete"],
            outputs="begin_augmenting_product_build_data",
            name="begin_augment_proudct_build_pipeline_connector",
            ),
    ])
