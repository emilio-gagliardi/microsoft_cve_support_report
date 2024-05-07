"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines()
    pipelines["__default__"] = sum(pipelines.values())
    pipelines["source_etl"] = (
        pipelines["__default__"]
        - pipelines["preprocess_docs_llama_index"]
        - pipelines["docstore_feature_engineering_pm"]
        - pipelines["docstore_classification_msrc"]
        - pipelines["knowledge_graph_extraction"]
        - pipelines["refresh_llama_vector_index"]
        - pipelines["ingest_product_build_cve_data"]
        - pipelines["augment_msrc_product_build_data"]
        - pipelines["periodic_report_CVE_WEEKLY_v1"]
    )
    pipelines["load_docstore"] = (
        pipelines["preprocess_docs_llama_index"]
    )
    pipelines["docstore_feature_engineering"] = (
        pipelines["docstore_feature_engineering_pm"]
        + pipelines["docstore_classification_msrc"]
    )
    pipelines["knowledge_graph_extraction"] = (
        pipelines["knowledge_graph_extraction"]
    )
    pipelines["refresh_llama_vector_index"] = (
        pipelines["refresh_llama_vector_index"]
    )
    pipelines["report_etl"] = (
        pipelines['ingest_rss_data']
        + pipelines["ingest_edge_releases"]
        + pipelines["ingest_patchmanagement"]
        + pipelines["augment_rss_data"]
        + pipelines["augment_edge_releases"]
        + pipelines["augment_patchmanagement"]
        + pipelines["transform_documents"]
        + pipelines["consolidate_stage_index_data"]
        + pipelines["preprocess_docs_llama_index"]
        + pipelines["ingest_product_build_cve_data"]
        + pipelines["augment_msrc_product_build_data"]
        + pipelines["docstore_classification_msrc"]
        + pipelines["docstore_feature_engineering_pm"]
    )
    return pipelines
