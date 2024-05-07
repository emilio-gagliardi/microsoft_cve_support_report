from kedro.framework.hooks import hook_impl
from kedro.pipeline.node import Node
from kedro.io import DataCatalog
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro_workbench.utils.constants import Status
import json
import hashlib
import logging
from pymongo import UpdateOne
from bson.objectid import ObjectId
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


conf_loader = ConfigLoader(settings.CONF_SOURCE)
credentials = conf_loader["credentials"]
mongo_atlas = credentials["mongo_atlas"]
username = mongo_atlas["username"]
password = mongo_atlas["password"]

class WriteClassificationResultsHook:
    
    @hook_impl
    def after_node_run(self, node: Node, catalog: DataCatalog, inputs: dict, outputs: dict):
        if node.name == "classify_post_msrc_node":
            # time.sleep(5)
            output_name = "classification_data_msrc"
            # df = catalog.load(output_name)
            df = outputs[output_name]
            # print(f"hook loaded dataframe with shape: {df.shape}")
            # Create a timestamp variable that holds the time as yyyy-mm-dd-hhmm
            timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
            json_data = df['classification_payload'].tolist()
            # print(f"hook generated a list of dicts with len: {len(json_data)}")
            # Extracting 'payload' column and writing to JSON file
            file_name = f"data/04_feature/classification_results_msrc_{timestamp}.json"
            with open(file_name, 'w') as file:
                json.dump(json_data, file, indent=4)
            logger.info(f"[HOOK]AfterNodeRun - Storing classification results to file: {file_name}")
                
    @hook_impl
    def after_node_run(self, node: Node, catalog: DataCatalog, inputs: dict, outputs: dict):
        if node.name == "classify_emails_node":
            # time.sleep(5)
            output_name = "feature_engineering_data_patch"
            # df = catalog.load(output_name)
            df = outputs[output_name]
            if not df.empty:
                # print(f"hook loaded dataframe with shape: {df.shape}")
                # Create a timestamp variable that holds the time as yyyy-mm-dd-hhmm
                timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
                json_data = df['classification_payload'].tolist()
                # print(f"hook generated a list of dicts with len: {len(json_data)}")
                # Extracting 'payload' column and writing to JSON file
                file_name = f"data/04_feature/classification_results_patch_{timestamp}.json"
                with open(file_name, 'w') as file:
                    json.dump(json_data, file, indent=4)
                    
                logger.info(f"[HOOK]AfterNodeRun - Storing classification results to file: {file_name}")

class WriteExtractionResultsHook:
    
    @hook_impl
    def after_node_run(self, node: Node, catalog: DataCatalog, inputs: dict, outputs: dict):
        if node.name == "save_entities_relationships":
            # time.sleep(5)
            output_name = "knowledge_graph_entities"
            # df = catalog.load(output_name)
            df = outputs[output_name]
            # print(f"hook loaded dataframe with shape: {df.shape}")
            # Create a timestamp variable that holds the time as yyyy-mm-dd-hhmm
            timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
            json_data = df['classification_payload'].tolist()
            # print(f"hook generated a list of dicts with len: {len(json_data)}")
            # Extracting 'payload' column and writing to JSON file
            file_name = f"data/04_feature/classification_results_msrc_{timestamp}.json"
            with open(file_name, 'w') as file:
                json.dump(json_data, file, indent=4)
            logger.info(f"[HOOK]AfterNodeRun -  Storing llm -> kg extraction results to file: {file_name}")

class WriteSummarizationResultsHook:
    
    @hook_impl
    def after_node_run(self, node: Node, catalog: DataCatalog, inputs: dict, outputs: dict):
        if node.name == "summarize_section_1_periodic_report_CVE_WEEKLY_v1":
            mongo = MongoDBDocs(
                mongo_db="report_docstore",
                mongo_collection="docstore",
                credentials={"username": username, "password": password}
            )
            output_name = "periodic_report_CVE_WEEKLY_v1_section_1_data_summary_df"
            # df = catalog.load(output_name)
            df = outputs[output_name]
            
            for index, row in df.iterrows():
                doc_id = row['id']
                summary = row['summary']
                update_results = mongo.collection.update_one({"id_": doc_id}, {"$set": {"metadata.summary": summary}})
                # print("Matched:", update_results.matched_count)
                # print("Modified:", update_results.modified_count)
            mongo.client.close()
            logger.info(f"[HOOK]AfterNodeRun - saving llm results to MongoDB: {df.shape}")

class UpdateRestartBehaviorMonitoringHook:
    @hook_impl
    def after_node_run(self, node: Node, catalog: DataCatalog, inputs: dict, outputs: dict):
        if node.name == "parse_restructure_installation_details":
            # Connect to the monitoring collection
            monitoring_collection = MongoDBDocs(
                mongo_db="report_docstore",
                mongo_collection="field_value_monitoring",
                credentials={"username": username, "password": password}
            )
            
            # Fetch the monitoring document for "restart_behavior"
            monitoring_doc = monitoring_collection.collection.find_one({'field_name': "restart_behavior"})
            # print(f"[HOOK] has monitoring doc -> {monitoring_doc}")
            
            # Extract the list of dicts from the output of the node
            output_name = "update_packages_with_installation_details"
            list_of_dicts = outputs[output_name]
            # print(f"[HOOK] has dataset of type {type(list_of_dicts)} with len: {len(list_of_dicts)}")
            
            # Bulk operations list
            bulk_operations = []

            # Iterate over each update_package document
            for doc in list_of_dicts:
                # print(f"[HOOK] has doc -> {doc}")
                for package in doc.get('downloadable_packages', []):
                    value_of_interest = package.get('restart_behavior', None)
                    unique_string = f"{doc['id']}-{package.get('product_name', '')}-{package.get('product_version', '')}-{package.get('product_architecture', '')}-{package.get('update_type', '')}"
                    hash_value = hashlib.md5(unique_string.encode()).hexdigest()

                    # Check if the value and hash combination is already monitored
                    is_processed = any(
                        ref for val in monitoring_doc.get('values', []) for ref in val.get('references', [])
                        if ref.get('doc_id') == str(doc['id']) and ref.get('hash') == hash_value
                    )

                    if not is_processed:
                        # Proceed if the package hasn't been processed
                        value_entry = next((item for item in monitoring_doc.get('values', []) if item['value'] == value_of_interest), None)
                        
                        if value_entry:
                            # Known value, update count and references
                            value_entry['count'] += 1
                            value_entry['last_updated'] = datetime.now(timezone.utc)
                        else:
                            # New value, create entry
                            value_entry = {
                                'value': value_of_interest,
                                'count': 1,
                                'first_seen': datetime.now(timezone.utc),
                                'last_updated': datetime.now(timezone.utc),
                                'status': Status.AWAITING_APPROVAL.value,  # Use the enum value directly
                                'references': []  # Initialize references for new value entries
                            }
                            monitoring_doc['values'].append(value_entry)
                            logger.info(f"\n===========================================\n[HOOK] RESTART_BEHAVIOR has New value: {value_entry['value']}\n===========================================\n")
                        # Append new reference including the hash
                        value_entry['references'].append({
                            'doc_id': str(doc['id']),
                            'collection_name': "microsoft_update_packages",
                            'date_added': doc['published'],
                            'hash': hash_value  # Include the hash in the reference
                        })

                        # Prepare the update operation
                        bulk_operations.append(UpdateOne(
                            {'_id': monitoring_doc['_id']},
                            {'$set': {'values': monitoring_doc['values']}}
                        ))

            # Execute bulk updates
            if bulk_operations:
                print(f"[HOOK] RESTART_BEHAVIOR has {len(bulk_operations)} updates to perform in FIELD_VALUE_MONITORING")
                monitoring_collection.collection.bulk_write(bulk_operations)
                
                
project_hooks = [WriteClassificationResultsHook(), WriteSummarizationResultsHook(), UpdateRestartBehaviorMonitoringHook()]