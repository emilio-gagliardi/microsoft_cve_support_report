"""
This is a boilerplate pipeline 'knowledge_graph_extraction'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (extract_posts_to_kg_extract,mongo_docs_to_dataframe,get_extraction_prompts, build_extraction_prompt_templates, fit_prompt_templates_to_window, extract_entities_from_collections, build_training_data_for_kg_extraction, save_entities_relationships, generate_cypher,save_cypher_statements_intermediate, augment_cypher_statements_node,save_cypher_statements_primary, load_cypher_statements,update_docstore_metadata_graph)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=extract_posts_to_kg_extract,
            inputs=["params:collections_for_knowledge_graph_extraction","params:mongo_projections_for_kg_extraction","params:report_end_date", "params:day_interval", "params:document_limit"],
            outputs="docs_for_kg_generation_list",
            name="extract_posts_to_kg_extract",
            ),
        node(
            func=mongo_docs_to_dataframe,
            inputs=["params:collections_for_knowledge_graph_extraction","docs_for_kg_generation_list"],
            outputs="docs_for_kg_generation_df",
            name="mongo_docs_to_dataframe",
            ),
        node(
            func=get_extraction_prompts,
            inputs=["params:collections_for_knowledge_graph_extraction"],
            outputs="kg_extraction_prompts",
            name="get_extraction_prompts",
            ),
        node(
            func=build_extraction_prompt_templates,
            inputs=["params:collections_for_knowledge_graph_extraction", "kg_extraction_prompts", "docs_for_kg_generation_df"],
            outputs="built_prompts_for_docs",
            name="build_extraction_prompt_templates",
            ),
        node(
            func=fit_prompt_templates_to_window,
            inputs=["built_prompts_for_docs", "params:max_prompt_tokens_kg_extraction"],
            outputs="fitted_prompts_for_docs",
            name="fit_prompt_templates_to_window",
            ),
        node(
            func=extract_entities_from_collections,
            inputs=["params:collections_for_knowledge_graph_extraction", "kg_extraction_prompts", "fitted_prompts_for_docs","params:report_end_date", "params:day_interval", "params:document_limit", "params:post_extraction_models", "params:edge_notes_extraction_models","params:patch_extraction_models", "params:extraction_llm_kwargs"],
            outputs="kg_entities",
            name="extract_entities_from_collections",
            ),
        node(
            func=save_entities_relationships,
            inputs=["kg_entities"],
            outputs="knowledge_graph_entities",
            name="save_entities_relationships",
            ),
        node(
            func=generate_cypher,
            inputs=["params:collections_for_knowledge_graph_extraction","kg_entities"],
            outputs=["cypher_statements_e_intermediate","cypher_statements_r_intermediate"],
            name="generate_cypher",
            ),
        node(
            func=build_training_data_for_kg_extraction,
            inputs=["params:collections_for_knowledge_graph_extraction","fitted_prompts_for_docs", "docs_for_kg_generation_df","kg_entities"],
            outputs=["kg_extraction_llm_results_msrc", "kg_extraction_llm_results_patch"],
            name="build_training_data_for_kg_extraction",
            ),
        node(
            func=save_cypher_statements_intermediate,
            inputs=["cypher_statements_e_intermediate","cypher_statements_r_intermediate"],
            outputs="cypher_statements_intermediate",
            name="save_cypher_statements_intermediate",
            ),
        node(
            func=augment_cypher_statements_node,
            inputs=["cypher_statements_intermediate"],
            outputs="cypher_statements_augmented",
            name="augment_cypher_statements_node",
            ),
        node(
            func=save_cypher_statements_primary,
            inputs=["cypher_statements_augmented"],
            outputs="cypher_statements_primary",
            name="save_cypher_statements_primary",
            ),
        node(
            func=load_cypher_statements,
            inputs=["cypher_statements_primary"],
            outputs="load_result",
            name="load_cypher_statements",
            ),
        node(
            func=update_docstore_metadata_graph,
            inputs=["params:collections_for_knowledge_graph_extraction","docs_for_kg_generation_df","load_result"],
            outputs=None,
            name="update_docstore_metadata_graph",
        )
    ])
