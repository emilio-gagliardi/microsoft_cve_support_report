"""
This is a boilerplate pipeline 'periodic_report_CVE_WEEKLY_v1'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (fetch_section_1_periodic_report_CVE_WEEKLY_v1_data,transform_section_1_periodic_report_CVE_WEEKLY_v1_data, fetch_section_1_periodic_report_CVE_WEEKLY_v1_product_build_data, fetch_section_1_periodic_report_CVE_WEEKLY_v1_update_packages_data, build_user_prompt_data_periodic_report_CVE_WEEKLY_v1,  build_periodic_report_CVE_WEEKLY_v1_data_container, fit_prompt_data_periodic_report_CVE_WEEKLY_v1, summarize_section_1_periodic_report_CVE_WEEKLY_v1, calculate_section_1_periodic_report_CVE_WEEKLY_v1, fetch_section_2_periodic_report_CVE_WEEKLY_v1_data, fetch_section_3_periodic_report_CVE_WEEKLY_v1_data, fetch_section_4_periodic_report_CVE_WEEKLY_v1_data, fetch_section_5_periodic_report_CVE_WEEKLY_v1_data,calculate_section_2_periodic_report_CVE_WEEKLY_v1,calculate_section_3_periodic_report_CVE_WEEKLY_v1, calculate_section_4_periodic_report_CVE_WEEKLY_v1,calculate_section_5_periodic_report_CVE_WEEKLY_v1, compile_periodic_report_CVE_WEEKLY_v1, fetch_section_6_periodic_report_CVE_WEEKLY_v1_data, calculate_section_6_periodic_report_CVE_WEEKLY_v1, save_periodic_report_CVE_WEEKLY_v1_data, generate_periodic_report_CVE_WEEKLY_v1_html, move_cve_weekly_report_assets_to_blob, load_report_assets_to_webserver)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
                func=fetch_section_1_periodic_report_CVE_WEEKLY_v1_data,
                inputs=["params:product_build_product_patterns","params:report_end_date", "params:day_interval", "params:document_limit"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_msrc_data_list",
                name="fetch_section_1_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=fetch_section_1_periodic_report_CVE_WEEKLY_v1_product_build_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_msrc_data_list","params:report_end_date", "params:day_interval", "params:product_build_product_patterns"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_product_build_df",
                name="fetch_section_1_periodic_report_CVE_WEEKLY_v1_product_build_data",
            ),
        node(
                func=fetch_section_1_periodic_report_CVE_WEEKLY_v1_update_packages_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_product_build_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_update_packages_df",
                name="fetch_section_1_periodic_report_CVE_WEEKLY_v1_update_packages_data",
            ),
        node(
                func=transform_section_1_periodic_report_CVE_WEEKLY_v1_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_msrc_data_list", "params:cve_metadata_keys_to_keep", 
                        "params:transformed_columns_to_keep",
                        "periodic_report_CVE_WEEKLY_v1_section_1_product_build_df","periodic_report_CVE_WEEKLY_v1_section_1_update_packages_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_data_df",
                name="transform_section_1_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=build_user_prompt_data_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_df", "params:user_prompt_metadata_keys_periodic_report_CVE_WEEKLY_v1"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_data_prompts_df",
                name="build_user_prompt_data_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=build_periodic_report_CVE_WEEKLY_v1_data_container,
                inputs=["params:report_end_date", "params:day_interval", "params:report_params_periodic_report_CVE_WEEKLY_v1"],
                outputs="periodic_report_CVE_WEEKLY_v1_data_container",
                name="build_periodic_report_CVE_WEEKLY_v1_data_container",
            ),
        node(
                func=fit_prompt_data_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_prompts_df", "params:max_prompt_tokens_periodic_report_CVE_WEEKLY_v1"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_data_fitted_df",
                name="fit_prompt_data_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=summarize_section_1_periodic_report_CVE_WEEKLY_v1,
                inputs=["params:periodic_report_CVE_WEEKLY_v1_summarization_models.default", "periodic_report_CVE_WEEKLY_v1_section_1_data_fitted_df", "params:max_llm_output_tokens_periodic_report_CVE_WEEKLY_v1", "params:llm_temperature_periodic_report_CVE_WEEKLY_v1"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_data_summary_df",
                name="summarize_section_1_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=calculate_section_1_periodic_report_CVE_WEEKLY_v1,
                inputs=["params:report_end_date", "params:day_interval","periodic_report_CVE_WEEKLY_v1_section_1_data_summary_df", "params:section_1_periodic_report_CVE_WEEKLY_v1.weekdays_order"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_1_data_primary",
                name="calculate_section_1_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=fetch_section_2_periodic_report_CVE_WEEKLY_v1_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_2_data_df",
                name="fetch_section_2_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=fetch_section_3_periodic_report_CVE_WEEKLY_v1_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_3_data_df",
                name="fetch_section_3_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=fetch_section_4_periodic_report_CVE_WEEKLY_v1_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_4_data_df",
                name="fetch_section_4_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=fetch_section_5_periodic_report_CVE_WEEKLY_v1_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_5_data_df",
                name="fetch_section_5_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=fetch_section_6_periodic_report_CVE_WEEKLY_v1_data,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_6_data_df",
                name="fetch_section_6_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=calculate_section_2_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_2_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_2_data_primary",
                name="calculate_section_2_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=calculate_section_3_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_3_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_3_data_primary",
                name="calculate_section_3_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=calculate_section_4_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_4_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_4_data_primary",
                name="calculate_section_4_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=calculate_section_5_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_5_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_5_data_primary",
                name="calculate_section_5_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=calculate_section_6_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_6_data_df"],
                outputs="periodic_report_CVE_WEEKLY_v1_section_6_data_primary",
                name="calculate_section_6_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=compile_periodic_report_CVE_WEEKLY_v1,
                inputs=["periodic_report_CVE_WEEKLY_v1_section_1_data_primary", "periodic_report_CVE_WEEKLY_v1_section_2_data_primary","periodic_report_CVE_WEEKLY_v1_section_3_data_primary","periodic_report_CVE_WEEKLY_v1_section_4_data_primary","periodic_report_CVE_WEEKLY_v1_section_5_data_primary", "periodic_report_CVE_WEEKLY_v1_section_6_data_primary", "periodic_report_CVE_WEEKLY_v1_data_container"],
                outputs="periodic_report_compiled",
                name="compile_periodic_report_CVE_WEEKLY_v1",
            ),
        node(
                func=save_periodic_report_CVE_WEEKLY_v1_data,
                inputs="periodic_report_compiled",
                outputs="periodic_report_saved",
                name="save_periodic_report_CVE_WEEKLY_v1_data",
            ),
        node(
                func=generate_periodic_report_CVE_WEEKLY_v1_html,
                inputs="periodic_report_saved",
                outputs="periodic_report_generated",
                name="generate_periodic_report_CVE_WEEKLY_v1_html",
            ),
        node(
                func=move_cve_weekly_report_assets_to_blob,
                inputs="periodic_report_generated",
                outputs=None,
                name="move_cve_weekly_report_assets_to_blob",
            ),
        node(
                func=load_report_assets_to_webserver,
                inputs=["periodic_report_generated", "params:report_params_periodic_report_CVE_WEEKLY_v1"],
                outputs=None,
                name="load_report_assets_to_webserver",
            ),
        
    ])
