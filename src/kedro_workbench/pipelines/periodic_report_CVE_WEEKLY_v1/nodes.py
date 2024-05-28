"""
This is a boilerplate pipeline 'periodic_report_CVE_WEEKLY_v1'
generated using Kedro 0.18.11
"""
import base64
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro_workbench.extras.datasets.SFTPSiteGround import SftpDataset
from kedro_workbench.utils.json_utils import mongo_docs_to_dataframe
from kedro_workbench.utils.kedro_utils import convert_to_actual_type
import kedro_workbench.utils.feature_engineering as feat_eng
from kedro_workbench.utils.periodic_report_CVE_WEEKLY_v1_prompts import seven_day_periodic_report_CVE_WEEKLY_v1_prompt_strings as prompt_strings
from kedro_workbench.utils.report_utils import (format_build_numbers, sort_products, fetch_and_merge_kb_data, fetch_package_pairs, record_report_total, plot_running_average, clean_appendix_title, split_product_name_string, filter_source_documents, prepare_plot_data, generate_and_save_plot, collect_metadata, build_mongo_pipeline, fetch_documents, add_category_to_documents, convert_to_utc_datetime, fetch_cve_documents_by_product_patterns, identify_exclusive_cve_documents, fetch_product_build_documents_by_product_patterns, annotate_package_pairs, remove_empty_package_pairs, create_thumbnail, find_file_path, extract_file_name, is_second_tuesday)
from kedro_workbench.utils.llm_utils import (create_metadata_string_for_user_prompt, fit_prompt_to_window,  apply_summarization)
from kedro_workbench.utils.sftp_utils import build_file_mappings
from kedro_workbench.utils.sendgrid_utils import (get_all_lists, get_recipients_from_sendgrid_list, load_encoded_file)
# import kedro_workbench.utils.yaml_utils as yaml_utils
from kedro.config import ConfigLoader
from kedro.framework.project import settings
import logging
from datetime import datetime, timedelta
import pandas as pd
from pandas.api.types import CategoricalDtype
import openai
from tqdm import tqdm
import json
import ast
from neo4j import GraphDatabase
from jinja2 import Environment, FileSystemLoader
import os
import subprocess
import pytz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Email, To, Content, Attachment, FileContent, FileName, FileType, Disposition,TrackingSettings, ClickTracking)


logger = logging.getLogger(__name__)
conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
mongo_atlas = credentials["mongo_atlas"]
username = mongo_atlas["username"]
password = mongo_atlas["password"]
open_ai_credentials = credentials["OPENAI"]
openai.api_key = open_ai_credentials["api_key"]
sendgrid_credentials = credentials["sendgrid"]
sendgrid_username = sendgrid_credentials["username"]
sendgrid_password = sendgrid_credentials["password"]
sendgrid_api_key = sendgrid_credentials["api_key"]


def clean_bad_build_data(report_end_date, day_interval, document_limit):
    # convert 'None' string to None type 
    document_limit = convert_to_actual_type(document_limit)
        
    mongo = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="docstore",
        credentials={"username": username, "password": password}
    )
    logger.info(f"Report period - end date: {report_end_date}, day interval: {day_interval}, document limit: {document_limit}")
    # Convert report_end_date to datetime object
    utc = pytz.UTC
    report_end_date_dt = datetime.strptime(report_end_date + " 23:59:59", "%d-%m-%Y %H:%M:%S")
    report_end_date_dt = utc.localize(report_end_date_dt)
    report_start_date_dt = report_end_date_dt - timedelta(days=day_interval)
    report_start_date_dt = report_start_date_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"report_start_date_dt: {report_start_date_dt}, report_end_date_dt: {report_end_date_dt}\n----------------------------")
    print("start cleaning bad data")
    # report_end_date_dt = datetime.strptime(report_end_date.replace("_", "-"), "%d-%m-%Y")
    # report_start_date_dt = report_end_date_dt - timedelta(days=day_interval)
    mongo_kb_articles = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_kb_articles",
        credentials={"username": username, "password": password}
    )
    mongo_product_builds = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_product_builds",
        credentials={"username": username, "password": password}
    )
    mongo_update_packages = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_update_packages",
        credentials={"username": username, "password": password}
    )
    match_query = {
        "metadata.collection": "msrc_security_update",
        "metadata.published": {
            "$gte": report_start_date_dt,
            "$lte": report_end_date_dt
        }
    }
    docstore_docs = mongo.collection.find(match_query)
    post_ids = [doc['metadata']['post_id'] for doc in docstore_docs]
    product_builds = mongo_product_builds.collection.find({
    "cve_id": {"$in": post_ids}
    })
    product_build_ids = [build['product_build_id'] for build in product_builds]
    mongo_kb_articles.collection.delete_many({
        "product_build_id": {"$in": product_build_ids}
    })
    mongo_update_packages.collection.delete_many({
        "product_build_id": {"$in": product_build_ids}
    })
    mongo_product_builds.collection.delete_many({
        "cve_id": {"$in": post_ids}
    })
    # Update operation to set the specified keys to empty arrays
    match_query = {
        "metadata.post_id": {"$in": post_ids}
    }
    update_operation = {
        "$set": {
            "metadata.build_numbers": [],
            "metadata.product_build_ids": [],
            "metadata.products": []
        }
    }

    # Perform the update
    result = mongo.collection.update_many(match_query, update_operation)
    print("completed removing bad data")
    
def fetch_section_1_periodic_report_CVE_WEEKLY_v1_data(products_to_keep_dict, report_end_date, day_interval, document_limit):
    # Note:products_to_keep_dict is defined in /parameters/ingest_product_build_cve_data.yml
    # convert 'None' string to None type 
    document_limit = convert_to_actual_type(document_limit)
        
    mongo = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="docstore",
        credentials={"username": username, "password": password}
    )
    docstore_collection = mongo.collection
    logger.info(f"Report period - end date: {report_end_date}, day interval: {day_interval}, document limit: {document_limit}")
    
    report_end_date_dt = convert_to_utc_datetime(report_end_date)
    report_start_date_dt = report_end_date_dt - timedelta(days=day_interval)
    print(f"report_start_date_dt: {report_start_date_dt}, report_end_date_dt: {report_end_date_dt}\n----------------------------")
    for key, products_list in products_to_keep_dict.items():
        products_to_keep_dict[key] = [split_product_name_string(product) for product in products_list]
    
    # print("windows 11 only")
    # Fetch Windows 11 exclusive documents
    windows_11_exclusive_docs, windows_11_exclusive_ids = identify_exclusive_cve_documents(
        docstore_collection, report_start_date_dt, report_end_date_dt,
        products_to_keep_dict['windows_11'],
        products_to_keep_dict['windows_10']
    )
    # print("\nedge only")
    # print(f"windows_shared_ids: {windows_11_exclusive_ids}\nkeep these docs\n{windows_11_exclusive_docs}")
    # Fetch Edge exclusive documents
    edge_exclusive_docs = fetch_cve_documents_by_product_patterns(
        docstore_collection, report_start_date_dt, report_end_date_dt,
        products_to_keep_dict['edge'], use_regex=True
    )
    # for doc in edge_exclusive_docs:
    #     print(f"{doc['metadata']['post_id']} - {doc['metadata']['products']}")
    edge_exclusive_ids = [doc['metadata']['id'] for doc in edge_exclusive_docs]
    
    # print("\nwindows shared")
    windows_shared_docs = fetch_cve_documents_by_product_patterns(
        docstore_collection, report_start_date_dt, report_end_date_dt,
        products_to_keep_dict['windows_10']+products_to_keep_dict['windows_11'],
        exclude_ids=windows_11_exclusive_ids + edge_exclusive_ids
    )

    add_category_to_documents(windows_11_exclusive_docs, 'windows_11_exclusive')
    add_category_to_documents(edge_exclusive_docs, 'microsoft_edge')
    add_category_to_documents(windows_shared_docs, 'windows_10_windows_11_shared')    
    
    section_1_data = windows_shared_docs + windows_11_exclusive_docs + edge_exclusive_docs
    # section_1_data = edge_exclusive_docs
    for doc in section_1_data:
    
        related_posts = []

        # Check each key in the metadata
        for key, value in doc.get("metadata", {}).items():
            # Check if the key matches the pattern (e.g., starts with "msrc_link:")
            if key.startswith("msrc_link:"):
                post_id = key.split(":", 1)[1]  # Extract the post_id from the key
                related_posts.append({post_id: value})

        # Update the document with the related_posts list
        doc["metadata"]["related_posts"] = related_posts
        if "summary" not in doc.get("metadata", {}):
            doc["metadata"]["summary"] = None
        if 'summarization_payload' not in doc.get("metadata", {}):
            doc["metadata"]['summarization_payload'] = None
    
    logger.info(f"Fetching data from MongoDB section 1: {len(section_1_data)} documents.")
    mongo.client.close()
    for item in section_1_data:
        # print(f"{item['metadata']['source']} - {item['metadata']['post_id']} - {item['metadata']['published']} - {item['metadata']['products']}  - {item['metadata']['description']}\n{item['text'][:50]}\n\n")
        print({item['metadata']['post_id']})   
    return section_1_data[:document_limit]
   

def fetch_section_1_periodic_report_CVE_WEEKLY_v1_product_build_data(cve_data, report_end_date, day_interval, products_to_keep_dict):
    if len(cve_data) == 0:
        return pd.DataFrame()

    report_end_date_dt = convert_to_utc_datetime(report_end_date)
    report_start_date_dt = report_end_date_dt - timedelta(days=day_interval+30)
    # create a list of CVE ids and fetch the product builds that contain those CVE ids
    mongo = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_product_builds",
        credentials={"username": username, "password": password}
    )
    product_builds_collection = mongo.collection
    
    cve_ids  = [cve['metadata']['post_id'] for cve in cve_data]
    # for cve in cve_data:
    #     print(f"{cve['metadata']['post_id']} - {cve['metadata']['build_numbers']}")
    # print(f"fetching product builds start date: {report_start_date_dt} end date:  {report_end_date_dt}")
    filtered_product_build_data_list = fetch_product_build_documents_by_product_patterns(
        product_builds_collection, None, report_end_date_dt, cve_ids,
        products_to_keep_dict['windows_10']+products_to_keep_dict['windows_11']+products_to_keep_dict['edge'], use_regex=False
    )
    
    # print(f"fetch product build data: {len(filtered_product_build_data_list)}\n")
    # for build in filtered_product_build_data_list:
    #     print(f"{build['product']} - {build['product_build_id']}")
    columns_to_keep = ['product', 'product_name', 'product_version', 'product_architecture', 'kb_id', 'cve_id', 'build_number', 'published', 'product_build_id', 'article_url', 'cve_url']  
    
    # print("\nprinting build numbers")
    # for item in filtered_product_build_data_list:
    #     print(f"{item['product']} - {item['build_number']} - {item['cve_id']} - {item['product_build_id']}")
    df = pd.DataFrame(filtered_product_build_data_list)
    # try:
    product_build_df = df[columns_to_keep].copy()
    # except KeyError:
    #     return pd.DataFrame()
    
    logger.info(f"Fetching Product Build data from MongoDB for section 1: {product_build_df.shape[0]} documents.")
    mongo.client.close()
    return product_build_df

def fetch_section_1_periodic_report_CVE_WEEKLY_v1_update_packages_data(product_build_data):
    
    if product_build_data.empty:
        return pd.DataFrame()
    
    mongo = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_update_packages",
        credentials={"username": username, "password": password}
    )
    
    product_build_ids = product_build_data['product_build_id'].tolist()
    # print(f"fetching update packages for build_ids:\n{product_build_ids}")
    # Define the query with date range filter
    query = {
        "product_build_id": {
            "$in": product_build_ids
        }
    }

    # Define the projection
    projection = {
        "_id": 0,
    }
    columns_to_keep = ['package_type', 'package_url', 'build_number', 'published', 'product_build_id', 'downloadable_packages']  
    # Fetch documents with sorting
    update_package_data_list = mongo.find_docs(query, projection)
    
    if update_package_data_list:
        df = pd.DataFrame(update_package_data_list)
        update_package_df = df[columns_to_keep].copy()
    else:
        update_package_df = pd.DataFrame(columns=['build_number', 'published', 'product_build_id', 'package_type', 'package_url', 'downloadable_packages'])
    # print(update_package_df.head())
    logger.info(f"Fetching Update Package data from MongoDB for section 1: {update_package_df.shape[0]} documents.")
    mongo.client.close()
    
    # for id, row in update_package_df.iterrows():
    #     item = row.to_dict()
        
    #     print(f"parent -> {item['product_build_id']} - {item['build_number']} - {item['package_url']}")
    #     for i, package in enumerate(item['downloadable_packages']):
    #         print(f"child package {i}")
    #         print(f"{package['product_name']} - {package['product_version']} - {package['product_architecture']} - {package['update_type']}")
    #     print()
    return update_package_df


def transform_section_1_periodic_report_CVE_WEEKLY_v1_data(msrc_docs, metadata_keys_to_keep, columns_to_keep, product_build_df, update_package_df):
    
    if len(msrc_docs) == 0:
        logger.info("### No MSRC posts to transform.")
        return pd.DataFrame(columns=columns_to_keep)
    elif len(msrc_docs) == 1 and msrc_docs[0] == 'None':
        logger.info("### Empty MSRC post, no data to transform.")
        return pd.DataFrame(columns=columns_to_keep)
    
    mongo = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="field_value_monitoring",
        credentials={"username": username, "password": password}
    )
    monitoring_collection = mongo.collection
    # mongo_docs_to_dataframe() assumes the llama index structured documents from docstore
    result_df = mongo_docs_to_dataframe(msrc_docs, metadata_keys=metadata_keys_to_keep)
    
    result_df['summarization_payload'] = pd.Series(dtype=object)
    result_df['revision'] = result_df['revision'].astype(float)
    result_df['published'] = result_df['published'].dt.strftime('%d-%m-%Y')
    result_df['published'] = result_df['published'].astype(str)
    result_df['build_number_str'] = result_df['build_numbers'].apply(format_build_numbers)
    result_df['core_products'] = result_df['products'].apply(sort_products)
    result_df['kb_article_pairs'] = result_df['post_id'].apply(lambda x: fetch_and_merge_kb_data(x, product_build_df))
    result_df['package_pairs'] = result_df['post_id'].apply(lambda x: fetch_package_pairs(x, product_build_df, update_package_df))
    result_df = result_df.apply(lambda row: annotate_package_pairs(row, monitoring_collection), axis=1)
    result_df['package_pairs'] = result_df['package_pairs'].apply(remove_empty_package_pairs)
    
    cat_type = CategoricalDtype(
        categories=["windows_11_exclusive", "windows_10_windows_11_shared", "microsoft_edge"],
        ordered=True
    )
    result_df['report_category'] = result_df['report_category'].astype(cat_type)
    result_df = result_df[columns_to_keep]
    # print("in transform")
    # for id, row in result_df.iterrows():
    #     print(f"{row['post_id']} - {row['report_category']}")
    mongo.client.close()
    logger.info(f"Built primary dataframe with {result_df.shape[0]} documents to process.")
    return result_df


def build_periodic_report_CVE_WEEKLY_v1_data_container(report_end_date, day_interval, report_params):
    
    end_date = datetime.strptime(report_end_date, "%d-%m-%Y")
    # Calculate report_start_date
    start_date = end_date - timedelta(days=day_interval)
    report_current_date = datetime.now().strftime("%d_%m_%Y")
    # Extract month and year
    report_day_start = start_date.strftime("%d")
    report_month_start = start_date.strftime("%B")
    report_year_start = start_date.strftime("%Y")
    report_day_end = end_date.strftime("%d")
    report_month_end = end_date.strftime("%B")
    report_year_end = end_date.strftime("%Y")

    report_data = {
        "report_day_start": report_day_start,
        "report_month_start": report_month_start,
        "report_year_start": report_year_start,
        "report_day_end": report_day_end,
        "report_month_end": report_month_end,
        "report_year_end": report_year_end,
        "report_end_date": datetime.strftime(end_date, "%Y_%m_%d"),
        "report_current_date": report_current_date,
        "report_title": report_params["report_title"],
        "report_subtitle": report_params["report_subtitle"],
        "report_description": report_params["report_description"],
        "plot_path": report_params["plot_path"],
        "toc": {},
        "sftp": {
            "html": [],
            "plots": []
        }
    }
    # print(type(report_params['section_labels']))
    for key, value in report_params['section_labels'].items():
        report_data[f'{key}_data'] = []
        report_data[f'{key}_metadata'] = {}
        report_data['toc'][key] = value
    
    logger.info(f"Report data container built.")
    
    return report_data


def build_user_prompt_data_periodic_report_CVE_WEEKLY_v1(data, metadata_keys):
    if data.empty:
        logger.info(f"CVE_WEEKLY_v1 skipping user prompts.")
        return data
    
    data['metadata_context'] = data.apply(lambda row: create_metadata_string_for_user_prompt(row, metadata_keys), axis=1)
    
    logger.info(f"CVE_WEEKLY_v1 prompts built.")
    
    return data

def fit_prompt_data_periodic_report_CVE_WEEKLY_v1(data, max_prompt_tokens):
    if data.empty:
        return data
    collection_label = "msrc_security_update"
    
    user_prompt_instructions = prompt_strings[collection_label]['user_prompt']
    separator = "\n------------------------\n"
    
    data['user_prompt'] = data.apply(lambda row: fit_prompt_to_window(user_prompt_instructions + separator + row['metadata_context'] + row['text'] + separator, max_prompt_tokens), axis=1)
    
    logger.info(f"User prompt fit to model token limit.")

    return data

def summarize_section_1_periodic_report_CVE_WEEKLY_v1(model, data, max_tokens, temperature):
    if data.empty:
        logger.info(f"No MSRC posts to summarize.")
        return data
    
    logger.info(f"using model: {model} to summarize msrc posts")
    # print(data.columns)
    if not data.empty:
        collection_label = "msrc_security_update"
        source_column_name = 'summarization_payload'
        key_to_extract = 'summary'
        system_prompt = prompt_strings[collection_label]['system_prompt']

        # Split the DataFrame into rows with and without summaries
        rows_with_summary = data[~data['summary'].isna() & (data['summary'] != '')]
        rows_without_summary = data[data['summary'].isna() | (data['summary'] == '')]

        # Check if rows_without_summary is empty
        if not rows_without_summary.empty:
            # If not empty, apply summarization to rows_without_summary
            summarized_rows = rows_without_summary.apply(
                lambda row: apply_summarization(row, model, system_prompt, max_tokens, temperature, source_column_name, key_to_extract), axis=1)
            # Concatenate the DataFrames back together
            updated_data = pd.concat([rows_with_summary, summarized_rows]).sort_index()
        else:
            # If rows_without_summary is empty, all rows already have summaries, so just use the original data
            updated_data = rows_with_summary.sort_index()

        # Sort updated_data by 'published' and 'post_id'
        updated_data = updated_data.sort_values(by=['published', 'post_id'], ascending=[True, False])
        logger.info(f"LLM Summarization complete. {updated_data.shape[0]} msrc posts summarized.")
        return updated_data
    else:
        raise ValueError("summarize_section_1_periodic_report_CVE_WEEKLY_v1 requires a non empty DataFrame as input.")
    
def calculate_section_1_periodic_report_CVE_WEEKLY_v1(report_end_date, day_interval, source_documents, weekdays_order):
    # source_documents is a DataFrame that contains MSRC posts
    # If there are no matching MSRC posts in this report period, we need to return data to ensure
    # the report can still be generated without any data.
    # this node function does the following:
    # - extracts the data necessary to generate a seaborn plot of frequency counts of MSRC posts by day
    # - calculates the total number of MSRC posts in the report period
    # - stores the data to be used in section 1 of the report in a dict that will be passed to the report generator node further down
    # - code is too combersome and needs to be refactored to make it easier to use and debug.
    # specifically, we need an easy way of changing the order of the day labels in the seaborn plot. Currently, the report is generated every Friday so the days on the plot start on Monday and end on Friday. However, we need to start on Tuesday now to align with Patch Tuesday by Microsoft
    if source_documents.empty:
        logger.info("No MSRC posts to plot by day.")
        return {"data": source_documents, "metadata": {'total_cves': 0}}
    
    source_documents = filter_source_documents(source_documents, report_end_date, day_interval)
    
    final_df = prepare_plot_data(source_documents, weekdays_order)
    
    report_end_date_dt = datetime.strptime(report_end_date, "%d-%m-%Y")
    timestamp = report_end_date_dt.strftime("%Y_%m_%d")
    filename = f"posts_by_day_{timestamp}.png"
    output_file_path = f"data/08_reporting/periodic_report_CVE_WEEKLY_v1/plots/{filename}"
    
    generate_and_save_plot(final_df, output_file_path, report_end_date, day_interval)
    
    logger.info(f"Generated daily post frequency plot: {output_file_path}")
    
    metadata = collect_metadata(source_documents)
    
    logger.info(f"Collected metadata data descriptives from source documents.")
    
    return_dict = {
        "data": source_documents,
        "metadata": metadata,
        "sftp": [output_file_path]
    }
    
    return return_dict

def fetch_section_2_periodic_report_CVE_WEEKLY_v1_data(source_documents):
    if source_documents.empty:
        return source_documents
    
    uri = "bolt://localhost:7687"  # Update with your Neo4j instance details
    username = "neo4j"
    password = "big.hat.group.password"  # Replace with your Neo4j password
    driver = GraphDatabase.driver(uri, auth=(username, password))
    mongo_ids = source_documents['id'].tolist()
    with driver.session() as session:
        result = session.run(
"""
MATCH (msrc:MSRCSecurityUpdate)-[r:AFFECTS_PRODUCT]->(ap:AffectedProduct)
WHERE msrc.mongo_id IN $mongo_ids
WITH ap, msrc.mongo_id AS mongo_id, count(r) AS relationship_count
ORDER BY relationship_count DESC, ap.name ASC
WITH ap, collect(mongo_id) AS mongo_ids, relationship_count
RETURN ap.id AS product_id, ap.name AS product_name, ap.version AS product_version, relationship_count, mongo_ids
""",
            {"mongo_ids": mongo_ids}
        )
        # list of tuples id, name, version, relationship_count
        records = [(record["product_id"], record["product_name"], record["product_version"], record["relationship_count"], record["mongo_ids"]) for record in result]
        # print(f"records: {records}")
        records_df = pd.DataFrame(records, columns=['product_id', 'product_name', 'product_version', 'relationship_count', 'mongo_id'])
        # print(f"records_df: {records_df.head()}")
    return records_df

def fetch_section_3_periodic_report_CVE_WEEKLY_v1_data(source_documents):
    if source_documents.empty:
        return source_documents
    
    uri = "bolt://localhost:7687"  # Update with your Neo4j instance details
    username = "neo4j"
    password = "big.hat.group.password"  # Replace with your Neo4j password
    driver = GraphDatabase.driver(uri, auth=(username, password))
    mongo_ids = source_documents['id'].tolist()
    entity_label = "Symptom"
    relationship_type = 'HAS_SYMPTOM'
    with driver.session() as session:
        query = (
"""
MATCH (msrc:MSRCSecurityUpdate)-[r:HAS_SYMPTOM]->(symptom:Symptom)
WHERE msrc.mongo_id IN $mongo_ids
WITH symptom, msrc.mongo_id AS mongo_id, collect(r.published_on) AS published_dates, count(symptom) AS symptom_count
WITH symptom, collect(mongo_id) AS mongo_ids, published_dates, symptom_count
RETURN symptom.id AS symptom_id, symptom.description AS symptom_description, symptom_count, mongo_ids, published_dates
ORDER BY symptom_count DESC
"""
        )
        result = session.run(query, {"mongo_ids": mongo_ids})
        symptom_records = [(record["symptom_id"], record["symptom_description"], record["symptom_count"], record["mongo_ids"], record["published_dates"]) for record in result]
        # print(f"symptom_records: {symptom_records}")
        records_df = pd.DataFrame(symptom_records, columns=['symptom_id', 'symptom_description', 'symptom_count', 'mongo_ids', 'published_dates'])
        records_df['published_dates'] = records_df['published_dates'].apply(lambda x: [datetime.strptime(date, '%d-%m-%Y') for date in x])
        records_df = records_df.explode('published_dates').sort_values(by='published_dates', ascending=False)
        # print(f"records_df: {records_df.head()}")
    return records_df

def fetch_section_4_periodic_report_CVE_WEEKLY_v1_data(source_documents):
    if source_documents.empty:
        return source_documents
    
    uri = "bolt://localhost:7687"  # Update with your Neo4j instance details
    username = "neo4j"
    password = "big.hat.group.password"  # Replace with your Neo4j password
    driver = GraphDatabase.driver(uri, auth=(username, password))
    mongo_ids = source_documents['id'].tolist()
    entity_label = "Fix"
    relationship_type = 'HAS_FIX'
    with driver.session() as session:
        query = (
"""
MATCH (msrc:MSRCSecurityUpdate)-[r:HAS_FIX]->(fix:Fix)
WHERE msrc.mongo_id IN $mongo_ids
WITH fix, msrc.mongo_id AS mongo_id, collect(r.published_on) AS published_dates
WITH fix, collect(mongo_id) AS mongo_ids, published_dates
RETURN fix.id AS fix_id, fix.description AS fix_description, mongo_ids, published_dates
"""
        )
        result = session.run(query, {"mongo_ids": mongo_ids})
        fix_records = [(record["fix_id"], record["fix_description"], record["mongo_ids"], record["published_dates"]) for record in result]

        # print(f"fix_records: {fix_records}")
        records_df = pd.DataFrame(fix_records, columns=['fix_id', 'fix_description', 'mongo_ids', 'published_dates'])
        records_df['published_dates'] = records_df['published_dates'].apply(lambda x: [datetime.strptime(date, '%d-%m-%Y') for date in x])
        records_df = records_df.explode('published_dates').sort_values(by='published_dates', ascending=False)
        # print(f"records_df: {records_df.head()}")
    return records_df

def fetch_section_5_periodic_report_CVE_WEEKLY_v1_data(source_documents):
    if source_documents.empty:
        return source_documents
    
    uri = "bolt://localhost:7687"  # Update with your Neo4j instance details
    username = "neo4j"
    password = "big.hat.group.password"  # Replace with your Neo4j password
    driver = GraphDatabase.driver(uri, auth=(username, password))
    mongo_ids = source_documents['id'].tolist()
    entity_label = "Tool"
    relationship_type = 'HAS_TOOL'
    with driver.session() as session:
        query = (
"""
MATCH (msrc:MSRCSecurityUpdate)-[r:HAS_TOOL]->(tool:Tool)
WHERE msrc.mongo_id IN $mongo_ids
WITH tool, msrc.mongo_id AS mongo_id, collect(r.published_on) AS published_dates
WITH tool, collect(mongo_id) AS mongo_ids, published_dates
RETURN tool.id AS tool_id, tool.description AS tool_description, tool.url AS tool_url, mongo_ids, published_dates
"""
        )
        result = session.run(query, {"mongo_ids": mongo_ids})
        tool_records = [(record["tool_id"], record["tool_description"], record["tool_url"], record["mongo_ids"], record["published_dates"]) for record in result]
        
        # Step 1: Create the DataFrame
        records_df = pd.DataFrame(tool_records, columns=['tool_id', 'tool_description', 'tool_url', 'mongo_ids', 'published_dates'])

        # Step 2: Convert string dates in lists to datetime objects
        records_df['published_dates'] = records_df['published_dates'].apply(lambda x: [datetime.strptime(date, '%d-%m-%Y') for date in x] if x else x)

        # Step 3: Explode 'published_dates'
        records_df = records_df.explode('published_dates')

        # Step 4: Convert datetime objects back to strings in the desired format
        records_df['published_dates'] = records_df['published_dates'].apply(lambda x: x.strftime('%d-%m-%Y') if pd.notna(x) else x)

        # Sort by 'published_dates'
        records_df = records_df.sort_values(by='published_dates', ascending=False)
        
        # print(f"tool data: {records_df.head()}")
    return records_df

# NEW CYPHER QUERY to extract all entities associated through patch management
def fetch_section_6_periodic_report_CVE_WEEKLY_v1_data(source_documents):
    if source_documents.empty:
        return source_documents
    
    uri = "bolt://localhost:7687"  # Update with your Neo4j instance details
    username = "neo4j"
    password = "big.hat.group.password"  # Replace with your Neo4j password
    driver = GraphDatabase.driver(uri, auth=(username, password))
    mongo_ids = source_documents['id'].tolist()

    with driver.session() as session:
        query = (
"""
MATCH (msrc:MSRCSecurityUpdate)
WHERE msrc.mongo_id IN $mongo_ids

// Retrieve Fixes and their related data
OPTIONAL MATCH (msrc)<-[r_ref_fix:REFERENCES]-(pm_fix:PatchManagement)-[r_fix_pm:HAS_FIX]->(fix_pm:Fix)
WITH msrc, collect(DISTINCT fix_pm) AS pm_fixes, collect(DISTINCT pm_fix.conversation_link) AS fix_conversation_links, collect(DISTINCT pm_fix.receivedDateTime) AS fix_receivedDateTimes

// Retrieve Tools and their related data
OPTIONAL MATCH (msrc)<-[r_ref_tool:REFERENCES]-(pm_tool:PatchManagement)-[r_tool_pm:HAS_TOOL]->(tool_pm:Tool)
WITH msrc, pm_fixes, fix_conversation_links, fix_receivedDateTimes, collect(DISTINCT tool_pm) AS pm_tools, collect(DISTINCT pm_tool.conversation_link) AS tool_conversation_links, collect(DISTINCT pm_tool.receivedDateTime) AS tool_receivedDateTimes

RETURN msrc.mongo_id AS mongo_id, 
       fix_conversation_links + tool_conversation_links AS conversation_links,
       fix_receivedDateTimes + tool_receivedDateTimes AS receivedDateTimes,
       pm_fixes, 
       pm_tools
"""
)
        patch_management_results = session.run(query, {"mongo_ids": mongo_ids})
        # patch_records = [(record["mongo_id"], record["conversation_links"], record["receivedDateTimes"], record["pm_fixes"], record["pm_tools"]) for record in patch_management_results]
        patch_records = [
            {
                'mongo_id': record['mongo_id'],
                'conversation_links': record['conversation_links'],
                'receivedDateTimes': record['receivedDateTimes'],
                'pm_fixes': record['pm_fixes'],
                'pm_tools': record['pm_tools']
            }
            for record in patch_management_results
        ]

        def extract_node_properties(node):
            try:
                if node is not None:
                    properties = dict(node)
                    if properties:
                        return properties
            except Exception as e:
                print(f"Error accessing properties: {e}")
            return None

        extracted_records = []
        for record in patch_records:
            mongo_id = record['mongo_id']
            conversation_links = record['conversation_links']
            receivedDateTimes = record['receivedDateTimes']
            pm_fixes = record['pm_fixes']
            pm_tools = record['pm_tools']

            conversation_links_list = [link for link in conversation_links if link is not None] if conversation_links else None
            receivedDateTimes_list = [datetime.strptime(dateTime, '%Y-%m-%dT%H:%M:%S%z').strftime('%d-%m-%Y') for dateTime in receivedDateTimes] if receivedDateTimes else None
            # Create a list of dictionaries for 'pm_fixes'
            fixes_list = [{'description': fix['description'], 'url': fix.get('url')} for fix in pm_fixes if fix is not None] if pm_fixes else None

            # Create a list of dictionaries for 'pm_tools'
            tools_list = [{'description': tool['description'], 'url': tool.get('url')} for tool in pm_tools if tool is not None] if pm_tools else None

            extracted_record = {
                'mongo_id': mongo_id,
                'conversation_links': conversation_links_list,
                'receivedDateTimes': receivedDateTimes_list,
                'pm_fixes': fixes_list,
                'pm_tools': tools_list
            }
            extracted_records.append(extracted_record)

        # Now you can iterate over extracted_records and print out the data
        # for idx, record in enumerate(extracted_records):
        #     print(f"Record {idx}:")
        #     print("mongo_id:", record['mongo_id'])
        #     conversation_links = record['conversation_links'] or []
        #     receivedDateTimes = record['receivedDateTimes'] or []
        #     print(f"Conversation link: {conversation_links}")
        #     print(f"Received DateTime: {receivedDateTimes}")
        #     print("Fixes:", record['pm_fixes'])
        #     print("Tools:", record['pm_tools'])
        #     print()

        column_names = ['mongo_id', 'conversation_links', 'receivedDateTimes', 'pm_fixes', 'pm_tools']
        records_df = pd.DataFrame(extracted_records, columns=column_names)
        # print("just made dataframe from patch related data")
        # print(records_df.head())
        # print(records_df.columns, records_df.shape)
        # filtered_df = records_df[records_df['pm_tools'].apply(lambda x: x is not None and len(x) > 0)]
        # print(filtered_df)
        # print(records_df.head())
        # print("passing to calculate")
        # Note. pm_fixes and pm_tools are lists of dictionaries
        
    return records_df
    
# End of NEW CYPHER QUERY

def calculate_section_2_periodic_report_CVE_WEEKLY_v1(section_2_data):
    if section_2_data.empty:
        return {}
    
    section_2_metadata = {"total_products": section_2_data.shape[0]}
    # print(f"section_2_metadata: {section_2_metadata}")
    return_data = {"data": section_2_data, "metadata": section_2_metadata}
    return return_data
    

def calculate_section_3_periodic_report_CVE_WEEKLY_v1(symptom_data):
    if symptom_data.empty:
        return {}
    
    symptom_metadata = {"total_symptoms": symptom_data.shape[0]}
    # print(f"symptom_metadata: {symptom_metadata}")
    return_data = {"data": symptom_data, "metadata": symptom_metadata}
    return return_data

def calculate_section_4_periodic_report_CVE_WEEKLY_v1(fix_data):
    if fix_data.empty:
        return {}
    
    fix_metadata = {"total_fixes": fix_data.shape[0]}
    # print(f"fix_metadata: {fix_metadata}")
    return_data = {"data": fix_data, "metadata": fix_metadata}
    return return_data

def calculate_section_5_periodic_report_CVE_WEEKLY_v1(section_3_data):
    if section_3_data.empty:
        section_3_metadata = {"total_tools": 0}
        return_data = {"data": section_3_data, "metadata": section_3_metadata}
        logger.info("No Tools Found.")
        return return_data
    section_3_metadata = {"total_tools": section_3_data.shape[0]}
    # print(f"section_3_metadata: {section_3_metadata}")
    return_data = {"data": section_3_data, "metadata": section_3_metadata}
    return return_data

def calculate_section_6_periodic_report_CVE_WEEKLY_v1(community_data):
    if community_data.empty:
        community_metadata = {"total_contributions":0, "community_fixes": 0, "community_tools": 0}
        return_data = {"data": community_data, "metadata": community_metadata}
        logger.info("No community contributions found.")
        return return_data
    
    total_tools = community_data['pm_tools'].apply(lambda x: len(x) if x is not None else 0).sum()
    total_fixes = community_data['pm_fixes'].apply(lambda x: len(x) if x is not None else 0).sum()
    community_metadata = {"total_contributions":total_tools+total_fixes ,"community_fixes":total_fixes, "community_tools": total_tools}
    # print(f"community_metadata: {community_metadata}")
    return_data = {"data": community_data, "metadata": community_metadata}
    return return_data

def compile_periodic_report_CVE_WEEKLY_v1(section_1_data, section_2_data, symptom_data, fix_data, section_3_data, community_data, report_data_container):
    # compiling all the data for the report. All of the data is stored in the key 'data' for each section
    # the data is being transformed and processed to make it easier to output with a jinja2 html template

    if section_1_data['data'].empty:
        return report_data_container
    
    columns_to_convert = ['build_number_str', 'kb_article_pairs', 'package_pairs', 'core_products']
    
    section_1_df = section_1_data['data'].copy()
    report_sftp = section_1_data.get('sftp')
    # Convert the specified columns
    for column in columns_to_convert:
        if column in section_1_df.columns:
            section_1_df[column] = section_1_df[column].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else x)
        else:
            logger.info(f"compile_periodic_report: Column '{column}' not found in the DataFrame.")
            section_1_df[column] = pd.Series([[] for _ in range(len(section_1_df))])
            
    cat_type = CategoricalDtype(
        categories=["windows_11_exclusive", "windows_10_windows_11_shared", "microsoft_edge"],
        ordered=True
    )
    section_1_df['report_category'] = section_1_df['report_category'].astype(cat_type)
    section_1_df = section_1_df.sort_values(['report_category', 'post_id'], ascending=[True, False])
    # for id, row in section_1_df.iterrows():
    #     id = row['post_id']
    #     category = row['report_category']
    #     package_pairs = row['package_pairs']
    #     print(f"{id} - {category}\n{package_pairs}")  
    section_1_df['revision'] = section_1_df['revision'].apply(lambda x: '{:.1f}'.format(x) if pd.notna(x) else "1.0")
    section_1_df['published'] = section_1_df['published'].dt.strftime('%d-%m-%Y')
    section_1_df['published'] = section_1_df['published'].astype(str)
    
    section_1_df.drop(columns=['metadata_context', 'user_prompt', 'weekday'], inplace=True)
    section_1_metadata = section_1_data['metadata']
    
    # products from neo4j graph database
    section_2_df = section_2_data.get('data', pd.DataFrame())
    section_2_list_of_dicts =[]
    
    if not section_2_df.empty:
        section_2_df['product_name'].replace('', pd.NA, inplace=True)
        section_2_df['product_version'].replace('', pd.NA, inplace=True)
        # cleanup version text, display empty string instead of 'Not provided' or 'None'
        section_2_df['product_version'] = section_2_df['product_version'].apply(
        lambda x: '' if x is None or x == 'Not provided' or x == 'None' else x)
        
        # print(f"section_2_df: {section_2_df.head()}")
        # explode mongo_ids so it can be joined with section_1_data
        exploded_section_2_df = section_2_df.explode('mongo_id')
        merged_section_1_df = pd.merge(section_1_df, exploded_section_2_df, left_on='id', right_on='mongo_id', how='left')
        grouped_df = merged_section_1_df.groupby('id')['product_name'].agg(lambda x: ', '.join(x.dropna().unique()))
        section_1_df['products'] = section_1_df['id'].map(grouped_df)
        unique_section_2_df = section_2_df.groupby(['product_name', 'product_version'])['product_id'].agg(list).reset_index()
        section_2_list_of_dicts = unique_section_2_df.to_dict('records')
    # print(section_2_list_of_dicts)
    section_2_metadata = section_2_data.get('metadata', {"total_products":0})
    section_2_metadata['total_products'] = len(section_2_list_of_dicts)
    
    # symptoms
    symptoms_df = symptom_data.get('data', pd.DataFrame())
    symptoms_list_of_dicts = []
    if not symptoms_df.empty:
        exploded_symptoms_df = symptoms_df.explode('mongo_ids')
        merged_section_1_df = pd.merge(section_1_df, exploded_symptoms_df, left_on='id', right_on='mongo_ids', how='left')
        grouped_df = merged_section_1_df.groupby('id')['symptom_description'].agg(lambda x: ', '.join(x.dropna().unique()))
        section_1_df['symptoms'] = section_1_df['id'].map(grouped_df)
        symptoms_list_of_dicts = symptoms_df.to_dict('records')
    
    symptoms_metadata = symptom_data.get('metadata', {})
    
    # fixes
    fixes_df = fix_data.get('data', pd.DataFrame())
    fixes_list_of_dicts = []
    if not fixes_df.empty:
        exploded_fixes_df = fixes_df.explode('mongo_ids')
        merged_section_1_df = pd.merge(section_1_df, exploded_fixes_df, left_on='id', right_on='mongo_ids', how='left')
        grouped_df = merged_section_1_df.groupby('id')['fix_description'].agg(lambda x: ', '.join(x.dropna().unique()))
        section_1_df['fixes'] = section_1_df['id'].map(grouped_df)
        fixes_list_of_dicts = fixes_df.to_dict('records')
    
    fixes_metadata = fix_data.get('metadata', {})
    
    # tools
    section_3_df = section_3_data.get('data', pd.DataFrame())
    section_3_list_of_dicts = []
    if not section_3_df.empty:
        section_3_df.dropna(subset=['tool_description', 'tool_url'], how='all', inplace=True)
        exploded_section_3_df = section_3_df.explode('mongo_ids')
        merged_section_1_df = pd.merge(section_1_df, exploded_section_3_df, left_on='id', right_on='mongo_ids', how='left')
        grouped_df = merged_section_1_df.groupby('id')['tool_description'].agg(lambda x: ', '.join(x.dropna().unique()))
        section_1_df['tools'] = section_1_df['id'].map(grouped_df)
        section_3_list_of_dicts = section_3_df.to_dict('records')
    
    section_3_metadata = section_3_data.get('metadata', {'total_tools':0})
    section_3_metadata['total_tools'] = len(section_3_list_of_dicts)
    
    # community data
    community_df = community_data.get('data', pd.DataFrame())
    community_list_of_dicts = []
    if not community_df.empty:
        section_1_df = pd.merge(section_1_df, community_df, left_on='id', right_on="mongo_id", how='left')
        section_1_df.drop(columns=['mongo_id'], inplace=True)
        list_columns = ['conversation_links', 'receivedDateTimes', 'pm_fixes', 'pm_tools']
        for col in list_columns:
            section_1_df[col] = section_1_df[col].apply(lambda x: x if isinstance(x, list) else [])
        print("final form of section 1 data before sending to jinja\n-----------\n")
        community_list_of_dicts = community_df.to_dict('records')
    
    commnunity_metadata = community_data.get('metadata', {})
    
    section_1_data_preprocessed = section_1_df.to_dict(orient='records')
    
    # temporary add-on fetch the windows 10 and 11 posts for the week and add to the json to test with streamlit
    mongo = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="docstore",
        credentials={"username": username, "password": password}
    )
    utc = pytz.UTC
    
    report_end_date_dt = datetime.strptime(report_data_container['report_end_date'] + " 23:59:59", "%Y_%m_%d %H:%M:%S")
    report_end_date_dt = utc.localize(report_end_date_dt)
    
    start_date_str = f"{report_data_container['report_year_start']}_{report_data_container['report_month_start']}_{report_data_container['report_day_start']}"
    report_start_date_dt = datetime.strptime(start_date_str, "%Y_%B_%d").replace(hour=0, minute=0, second=0, microsecond=0)

    appendix_collections = ['windows_10', 'windows_11', 'stable_channel_notes', 'security_update_notes']
       
    pipeline = [
        {
            "$match": {
                "metadata.collection": {
                    "$in": appendix_collections
                },
                "metadata.published": {
                    "$gte": report_start_date_dt,
                    "$lte": report_end_date_dt
                }
            }
        },
        {
            "$addFields": {
                "collection_order": {
                    "$indexOfArray": [appendix_collections, "$metadata.collection"]
                }
            }
        },
        {
            "$sort": {
                "collection_order": 1,
                "metadata.published": -1,
                "metadata.post_id": -1
            }
        },
        {
            "$project": {
                "_id": 0,
                "metadata.id": 1,
                "metadata.post_id": 1,
                "metadata.collection": 1,
                "metadata.published": {
                    "$dateToString": { "format": "%Y-%m-%d", "date": "$metadata.published" }
                },
                "metadata.title": 1,
                "metadata.source": 1,
                "metadata.subject": 1
            }
        },
        {
            "$group": {
                "_id": "$metadata.collection",
                "documents": { "$push": "$$ROOT" }
            }
        },
        {
            "$sort": {
                "_id": 1  # Optional: Sort the groups by collection name if needed
            }
        }
    ]

    results = mongo.collection.aggregate(pipeline)

    reshaped_documents = {}

    for group in results:
        # Initialize the list for the current group if not already done
        if group['_id'] not in reshaped_documents:
            reshaped_documents[group['_id']] = []

        for doc in group['documents']:
            metadata = doc['metadata']
            # Determine if 'title' or 'subject' exists and use the appropriate one
            title_or_subject = metadata.get('title', metadata.get('subject', 'No Title or Subject'))
            
            cleaned_title_or_subject = clean_appendix_title(title_or_subject)
            
            reshaped_document = {
                'id': metadata.get('id', 'No ID'),  # Using .get() for safe access
                'source': metadata.get('source', 'No Source'),
                'collection': group['_id'],  # The collection name is now the group ID
                'published': metadata.get('published', 'No Published Date'),
                'title': cleaned_title_or_subject  # Use the determined 'title' or 'subject'
            }

            reshaped_documents[group['_id']].append(reshaped_document)

    # reshaped_documents now structured as desired: {"collection_name": [list of reshaped dicts], ...}


    # This list is now correctly structured for further use, such as in a Jinja2 template
    section_4_list_of_dicts = reshaped_documents

    # for collection, docs in section_4_list_of_dicts.items():
    #     for doc in docs:
    #         print(doc)
    # end temporary add-on
    
    # build data container
    
    report_data_container['section_1_data'] = section_1_data_preprocessed
    report_data_container['section_1_metadata'] = section_1_metadata
    report_data_container['section_2_data'] = section_2_list_of_dicts
    report_data_container['section_2_metadata'] = section_2_metadata
    report_data_container['section_3_data'] = section_3_list_of_dicts
    report_data_container['section_3_metadata'] = section_3_metadata
    report_data_container['section_4_data'] = reshaped_documents
    report_data_container['sftp'] = report_sftp

    logger.info(f"CVE_WEEKLY_v1 report data built.")
    mongo.client.close()
    # for doc in section_1_data_preprocessed:
    #     print(doc['post_id'])
    #     package_pairs = doc['package_pairs']
    #     for pair in package_pairs:
    #         downloadables = pair['downloadable_packages']
    #         print(len(downloadables))
    #     print()
    
    return report_data_container
    
def save_periodic_report_CVE_WEEKLY_v1_data(report_data_container):
    
    date_as_string = f"{report_data_container['report_year_end']}_{report_data_container['report_month_end']}_{report_data_container['report_day_end']}"
    timestamp = datetime.strptime(date_as_string, '%Y_%B_%d').strftime('%Y_%m_%d')
    csv_file_name = f'data/08_reporting/periodic_report_CVE_WEEKLY_v1/metadata/weekly_totals.csv'
    
    if len(report_data_container['section_1_data']) > 0:
       
        json_file_name = f'data/08_reporting/periodic_report_CVE_WEEKLY_v1/json/periodic_report_CVE_WEEKLY_v1_{timestamp}.json'
        logger.info(f"Saving report data to {json_file_name}")
        # Writing the dictionary to a file in JSON format
        with open(json_file_name, 'w') as json_file:
            json.dump(report_data_container, json_file, indent=4)

        record_report_total(timestamp, report_data_container['section_1_metadata']['total_cves'], csv_file_name)
        
        logger.info(f"\nSaving report metadata to {csv_file_name}\n")
        
        df = pd.read_csv(csv_file_name)
        df['report_date'] = pd.to_datetime(df['report_date'], format='%Y_%m_%d')
        df.set_index('report_date', inplace=True)
        # Calculate the running average
        df['running_average'] = df['item_total'].expanding().mean()
        weekly_summary_plot_file_path = f'data/08_reporting/periodic_report_CVE_WEEKLY_v1/plots/'
        weekly_summary_plot_file_name = f'weekly_totals_{timestamp}.png'
        full_path = os.path.join(weekly_summary_plot_file_path, weekly_summary_plot_file_name)
        plot_running_average(df, full_path, timestamp.replace("_","-"))
        report_data_container['sftp'].append(full_path)
        logger.info(f"\nWeekly summary plot saved to {weekly_summary_plot_file_path}{weekly_summary_plot_file_name}\n")
    else:
        record_report_total(timestamp, 0, csv_file_name)
    
    return report_data_container
    

def generate_periodic_report_CVE_WEEKLY_v1_html(report_data_container):
    
    if len(report_data_container['section_1_data']) > 0:
        
        date_as_string = f"{report_data_container['report_year_end']}_{report_data_container['report_month_end']}_{report_data_container['report_day_end']}"
        timestamp = datetime.strptime(date_as_string, '%Y_%B_%d').strftime('%Y_%m_%d')
        report_data = {"data": report_data_container}
        
        try:
            # Template directory
            template_dir = 'data/09_templates/CVE_WEEKLY_v1/'
            env = Environment(loader=FileSystemLoader(template_dir))

            # Load the template
            template = env.get_template('periodic_report_template_v3.7.html')

            # Render the template with the data
            rendered_template = template.render(report_data)

            # Define the full path for the output file
            output_dir = 'data/08_reporting/periodic_report_CVE_WEEKLY_v1/html/'  
            html_file_name = f"periodic_report_CVE_WEEKLY_v1_{timestamp}.html"
            full_html_path = os.path.join(output_dir, f'{html_file_name}')
            absolute_html_path = os.path.abspath(full_html_path)
            report_data_container['sftp'].append(full_html_path)
            # Write the rendered template to the output file
            with open(full_html_path, 'w') as f:
                f.write(rendered_template)
            logger.info(f"HTML report generated successfully.")
            # create thumbnail for report
            thumbnail_path = "data/08_reporting/periodic_report_CVE_WEEKLY_v1/thumbnails/"
            thumbnail_filename = f"thumbnail_{timestamp}.png"
            full_thumbnail_path = os.path.join(thumbnail_path, thumbnail_filename)
            if not os.path.exists(thumbnail_path):
                os.makedirs(thumbnail_path)
            create_thumbnail(absolute_html_path, full_thumbnail_path)
            report_data_container['sftp'].append(full_thumbnail_path)
            logger.info(f"HTML report thumbnail generated successfully.")
            return report_data_container
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"HTML report generation failed. Error -> {e}")
            return {}
    else:
        return {}

def send_notification_to_sendgrid_qa_list(report_data_container, params):
    # Extract the path for the PNG and HTML files
    all_file_paths = report_data_container['sftp']
    thumbnail_path = find_file_path(all_file_paths, 'thumbnails')
    html_path = find_file_path(all_file_paths, 'html')

    # Load the PNG file content and encode it
    report_thumbnail = load_encoded_file(thumbnail_path)

    # Fetch configuration and list details
    all_lists = get_all_lists(sendgrid_api_key)
    report_qa_sendgrid_list = next((d for d in all_lists if d['name'] == 'msrc_weekly_report_qa_team'), None)
    report_qa_sendgrid_list_id = report_qa_sendgrid_list['id']

    # Construct email details
    notification_from = params['report_qa']['from']
    notification_subject = params['report_qa']['subject']
    notification_body_template = params['report_qa']['body']
    notification_to = get_recipients_from_sendgrid_list(sendgrid_api_key, report_qa_sendgrid_list_id)
    # print(f"Updated get_recipients() -> {notification_to}")
    notification_to = "emilio.gagliardi@portalfuse.io"
    report_end_date = report_data_container['report_end_date']
    contact_email = params['report_qa']['contact']
    report_base_url = params['report_qa']['base_url']
    html_file_name = extract_file_name(html_path)

    notification_body = notification_body_template.format(
        report_end_date=report_end_date,
        base_url=report_base_url,
        html_file_name=html_file_name,
        contact_email=contact_email
    )

    # Create and configure the email object
    email = Mail(from_email=notification_from, to_emails=notification_to, subject=notification_subject, plain_text_content=notification_body)
    attachment = Attachment(file_content=FileContent(report_thumbnail), file_type=FileType('image/png'), file_name=FileName(extract_file_name(thumbnail_path)), disposition=Disposition('attachment'))
    email.attachment = attachment

    tracking_settings = TrackingSettings(click_tracking=ClickTracking(enable=False, enable_text=False))
    email.tracking_settings = tracking_settings

    # Send the email
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(email)
        print(f"Email sent with status code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")


def create_draft_campaign_cve_weekly(report_data_container, params):
    # Extract the path for the PNG and HTML files
    all_file_paths = report_data_container['sftp']
    thumbnail_path = find_file_path(all_file_paths, 'thumbnails')
    html_path = find_file_path(all_file_paths, 'html')

    # Load the PNG file content and encode it
    # report_thumbnail = load_encoded_file(thumbnail_path)

    # Fetch configuration and list details
    all_lists = get_all_lists(sendgrid_api_key)
    report_subscriber_sendgrid_list = next((d for d in all_lists if d['name'] == 'msrc_weekly_report'), None)
    report_subscriber_sendgrid_list_id = report_subscriber_sendgrid_list['id']
    # print(f"report_subscriber_sendgrid_list: {report_subscriber_sendgrid_list}")
    # print(f"report_subscriber_sendgrid_list_id: {report_subscriber_sendgrid_list_id}")
    # Construct email details
    campaign_from = params['subscriber_campaign']['from']
    campaign_subject_template = params['subscriber_campaign']['subject']
    campaign_body_template = params['subscriber_campaign']['body']
    # campaign_to = get_recipients_from_sendgrid_list(sendgrid_api_key, report_subscriber_sendgrid_list_id)
    report_end_date = report_data_container['report_end_date']
    # print(f"report end date: {report_end_date}")
    report_base_url = params['subscriber_campaign']['base_url']
    # print(f"report_base_url: {report_base_url}")
    html_file_name = extract_file_name(html_path)
    # print(f"html_file_name: {html_file_name}")
    thumbnail_file_name = extract_file_name(thumbnail_path)
    # print(f"thumbnail_file_name: {thumbnail_file_name}")
    patch_tuesday = "[Patch Tuesday]" if is_second_tuesday(datetime.strptime(report_end_date, "%Y_%m_%d")) else ""
    # print(f"patch_tuesday: {patch_tuesday}")
    campaign_subject = campaign_subject_template.format(
        report_end_date=report_end_date,
        patch_tuesday=patch_tuesday
    )
    # print(f"rendered subject: {campaign_subject}")
    campaign_body = campaign_body_template.format(
        report_end_date=report_end_date,
        base_url=report_base_url,
        html_file_name=html_file_name,
        thumbnail_file_name=thumbnail_file_name
    )
    # print(f"rendered body: {campaign_body}")

    sender_id = 5660513
    # # Create the campaign draft
    # data = {
    #     "title": f"CVE Weekly Report Draft for {report_end_date}",
    #     "subject": campaign_subject,
    #     "sender_id": sender_id,
    #     "list_ids": [report_subscriber_sendgrid_list_id],
    #     "html_content": campaign_body,
    #     "custom_unsubscribe_url": ""
    # }
    data = {
        "name": f"CVE Weekly Report for {report_end_date}",
        "send_to": {"list_ids": [report_subscriber_sendgrid_list_id]},
        "email_config": {
            "subject": campaign_subject,
            "sender_id": sender_id,
            "html_content": campaign_body,
            "generate_plain_content": True,
            "editor": "code",
            "suppression_group_id": 28454}
        }

    print(data)

    sg = SendGridAPIClient(sendgrid_api_key)
    try:
        response = sg.client.marketing.singlesends.post(request_body=data)
        print(f"Campaign created successfully, Status Code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")

        # Inspect the exception object for more information
        if hasattr(e, 'body'):
            print(f"Error Body: {e.body}")
        if hasattr(e, 'status_code'):
            print(f"HTTP Status Code: {e.status_code}")
        if hasattr(e, 'headers'):
            print(f"Response Headers: {e.headers}")


def create_draft_campaign_cve_weekly_workaround(report_data_container, params):
    # the report data container contains a list of file paths for assets generated during generation. 
    # the order is arbitrary, so the list is extracted and then each asset's path is retreived
    all_file_paths = report_data_container['sftp']
    thumbnail_path = find_file_path(all_file_paths, 'thumbnails')
    html_path = find_file_path(all_file_paths, 'html')

    # Load the PNG file content and encode it
    # report_thumbnail = load_encoded_file(thumbnail_path)

    # SendGrid SDK is used to grab all list details
    # Fetch configuration and list details
    all_lists = get_all_lists(sendgrid_api_key)
    # We only want to send the report to subscribers, iterate over lists until report subscribers is found
    # this string should be parameterized in yaml and passed at runtime.
    report_subscriber_sendgrid_list = next((d for d in all_lists if d['name'] == 'msrc_weekly_report'), None)
    report_subscriber_sendgrid_list_id = report_subscriber_sendgrid_list['id']
    # print(f"report_subscriber_sendgrid_list: {report_subscriber_sendgrid_list}")
    # print(f"report_subscriber_sendgrid_list_id: {report_subscriber_sendgrid_list_id}")
    # Construct email details
    campaign_from = params['subscriber_campaign']['from']
    campaign_subject_template = params['subscriber_campaign']['subject']
    campaign_body_template = params['subscriber_campaign']['body']
    # campaign_to = get_recipients_from_sendgrid_list(sendgrid_api_key, report_subscriber_sendgrid_list_id)
    report_end_date = report_data_container['report_end_date']
    # print(f"report end date: {report_end_date}")
    report_base_url = params['subscriber_campaign']['base_url']
    # print(f"report_base_url: {report_base_url}")
    html_file_name = extract_file_name(html_path)
    # print(f"html_file_name: {html_file_name}")
    thumbnail_file_name = extract_file_name(thumbnail_path)
    # print(f"thumbnail_file_name: {thumbnail_file_name}")
    date_to_check = datetime.strptime(report_end_date, "%Y_%m_%d")
    patch_tuesday = " [Patch Tuesday]" if is_second_tuesday(date_to_check) else ""
    # print(f"patch_tuesday: {patch_tuesday}")
    campaign_subject = campaign_subject_template.format(
        report_end_date=report_end_date,
        patch_tuesday=patch_tuesday
    )
    # print(f"rendered subject: {campaign_subject}")
    campaign_body = campaign_body_template.format(
        report_end_date=report_end_date,
        base_url=report_base_url,
        html_file_name=html_file_name,
        thumbnail_file_name=thumbnail_file_name
    )
    # print(f"rendered body: {campaign_body}")
    
    message = Mail(
        from_email='webmaster@portalfuse.io',
        to_emails='emilio.gagliardi@portalfuse.io',
        subject=campaign_subject,
        html_content=campaign_body)
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        # print(response.status_code)
        # print(response.body)
        # print(response.headers)
    except Exception as e:
        print(e.message)

def move_cve_weekly_report_assets_to_blob(report_data_container):
    
    if report_data_container:
        ps_script_path = r"C:\ps_scripts\copy_cve_weekly_v1_to_blob.ps1"
        
        command = ["powershell.exe", "-ExecutionPolicy", "Unrestricted", "-File", ps_script_path]
        
        process = subprocess.run(command, capture_output=True, text=True)
        logger.info(f"Copied report assets to blob storage.")
        # print("STDOUT:", process.stdout)
        # print("STDERR:", process.stderr)
    else:
        logger.info(f"No report generated. Skipping moving report assets to blob storage.")


def load_report_assets_to_webserver(report_data_container, params):
    # manually instantiate the sftp dataset and perform operations
    # print(params.keys())
    if report_data_container:
        sftp_credentials = credentials['sftp_website']['ssh']
        sftp_payload = report_data_container['sftp']
        # print(f"creds: {sftp_credentials}\nruntime values\n{sftp_payload}")
        local_base_path = params['sftp']['local_base_path']
        remote_base_path = params['sftp']['remote_base_path']
        
        file_mappings = build_file_mappings(
            sftp_payload,
            remote_base_path
        )
        # print(f"file mappings\n{file_mappings}")
        sftp_conn = SftpDataset(credentials=sftp_credentials)
        sftp_conn._save(file_mappings)
        # output = sftp_conn.execute_command("cd ~/www/portalfuse.io/public_html && wp sg purge")
        # print("Command output:", output)
        sftp_conn._close()
    else:
        print("SFTP data empty")
    