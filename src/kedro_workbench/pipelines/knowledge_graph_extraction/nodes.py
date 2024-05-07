"""
This is a boilerplate pipeline 'knowledge_graph_extraction'
generated using Kedro 0.18.11
"""
import os
from openai import OpenAI
import tiktoken
# from retry import retry
import re
from string import Template
import json
import hashlib
from neo4j import GraphDatabase, exceptions
from timeit import default_timer as timer
from time import sleep
import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId
from pprint import pprint
from datetime import datetime, timedelta
import json
import spacy
# from httpx import patch
from numpy import extract
from llama_index.prompts import PromptTemplate
import pandas as pd
from tqdm import tqdm
from kedro.config import ConfigLoader
from kedro.framework.project import settings
import kedro_workbench.utils.kg_extraction_prompts as kg_extraction_prompts
import kedro_workbench.utils.kg_extraction_utils as kg_extract_utils
from kedro_workbench.utils.kg_extraction_utils import IdMappingManager
import kedro_workbench.utils.llm_utils as llm_utils
from kedro_workbench.utils.kedro_utils import convert_to_actual_type
import logging

logger = logging.getLogger(__name__)
conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]

username=credentials["mongo_atlas"]["username"]
password=credentials["mongo_atlas"]["password"]
mongo_url = f"mongodb+srv://{username}:{password}@bighatcluster.wamzrdr.mongodb.net/"
mongo_client = MongoClient(mongo_url)
db = mongo_client["report_docstore"]
db_collection = db["docstore"]

def extract_posts_to_kg_extract(collections, projections, end_date, day_interval, document_limit):
    limit = convert_to_actual_type(document_limit)
    report_end_date = datetime.strptime(end_date.replace("_","-"), "%d-%m-%Y")
    report_start_date = report_end_date - timedelta(days=day_interval)
    records_to_return = {}
    records_to_return["num_records"] = 0
    if limit is None:
        limit_value = 2147483647  
        batch_size = limit_value
    else:
        limit_value = limit
        batch_size = max(1, limit_value // len(collections))

    for collection in collections:
        projection_dict = projections[collection]
        # Define the date filter for 'metadata.published'
        date_filter = {
            "$match": {
                "metadata.published": {
                    "$gte": report_start_date,  # Greater than or equal to start date
                    "$lte": report_end_date     # Less than or equal to end date
                }
            }
        }

        # Define the existing match condition
        match_condition = {
            "metadata.collection": collection,
            "metadata.added_to_graph_store": False
        }

        if collection != "patch_management":
            pipeline = [
                {"$match": match_condition},
                date_filter,  # Add the date filter to the pipeline
                {"$project": projection_dict},
                {"$limit": batch_size}
            ]
        else:
            # Extend the match condition for the 'patch_management' collection
            match_condition["metadata.post_type"] = {"$in": ["Solution provided", "Problem statement", "Helpful tool"]}

            pipeline = [
                {"$match": match_condition},
                date_filter,  # Add the date filter to the pipeline
                {"$project": projection_dict},
                {"$sort": {"topic": 1, "receivedDateTime": 1}},
                {"$limit": batch_size}
            ]



        cursor = db_collection.aggregate(pipeline)
        collection_docs = list(cursor)
        # print(f"cursor is a {type(cursor)} and returns {len(collection_docs)} docs")
        if len(collection_docs) > 0:
            records_to_return[collection] = collection_docs
            print(f"collection: {collection} has {len(collection_docs)} docs\n")
            records_to_return["num_records"] += len(collection_docs)
    
    logger.info(f"Begin processing {records_to_return['num_records']} mongo records for kg generation")
    logger.info("Patch Management of type 'Conversational' are omitted from the graph store. So there will always be documents in mongo where 'metadata.added_to_graph_store' will always be False.")
    
    return records_to_return

def mongo_docs_to_dataframe(collections, mongo_docs):
    """
    Transforms a list of MongoDB documents into a Pandas DataFrame.

    Args:
        mongo_docs (dict): A list of dictionaries by collection, where each dictionary is a MongoDB document.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the MongoDB documents.
    """
    pd.set_option("display.max_rows", 600)
    pd.set_option("display.max_columns", 500)
    pd.set_option("max_colwidth", 400)
    metadata_keys = ['id', 'topic', 'subject', 'post_id', 'collection', 'revision', 'published', 'receivedDateTime', 'post_type', 'source', 'description','conversation_link', 'evaluated_keywords']
    collection_dfs = {}

    for collection in collections:
        # Filter mongo_docs for current collection
        if collection in mongo_docs:
            filtered_docs = [
                {**{key: doc['metadata'][key] for key in metadata_keys if key in doc['metadata']},
                **({'text': doc['text']} if 'text' in doc else {})}
                for doc in mongo_docs[collection]
            ]
            if len(filtered_docs) > 0:
                # Determine all possible column names
                all_column_names = set().union(*(d.keys() for d in filtered_docs))

                # Convert set of column names to list
                column_names_list = list(all_column_names)

                # Create a DataFrame for the current collection with all columns
                collection_dfs[collection] = pd.DataFrame(filtered_docs, columns=column_names_list)

    # if collection_dfs:
    #     collection_keys = list(collection_dfs.keys())

    #     for key in collection_keys:
    #         print(f"First 10 rows of the DataFrame in '{key}':")
    #         print(collection_dfs[key].head(10))
        
    return collection_dfs

def get_extraction_prompts(collections):
    # get the user and system prompts for each collection
    extraction_prompts = {}
    for collection in collections:
        prompt = getattr(kg_extraction_prompts, collection, None)
        extraction_prompts[collection] = prompt
    # collection_keys = list(extraction_prompts.keys())
    # for key in collection_keys:
    #     print(f"prompt for '{key}':")
    #     print(extraction_prompts[key])
    return extraction_prompts

def build_extraction_prompt_templates(collections, prompts, data):
    # create a user prompt for each record that includes the metadata and text
    # return a dictionary with keys for each collection and values that are lists of prompts
    built_prompts = {}
    metadata_keys = {"msrc_security_update": ['id', 'post_id', 'revision', 'published', 'post_type', 'source'],
                     "patch_management": ['id', 'topic', 'published', 'receivedDateTime', 'post_type', 'conversation_link'],}
    run_collections = data.keys()
    for collection in run_collections:
        documents_df = data[collection]
        # collection prompts contains a dictionary with 2 keys "user" and "system"
        collection_prompts = prompts[collection]
        # print(f"extracting entities for {len(documents_df)} docs in {collection}")
        # print(f"documents_df.columns: {documents_df.columns}")
        built_prompts[collection] = []
        for index, row in documents_df.iterrows():

            metadata_str = llm_utils.create_metadata_string_for_user_prompt(row, metadata_keys[collection])
            
            text = row["text"].rstrip() if pd.notna(row["text"]) else ""

            full_str = metadata_str + text
            
            user_prompt = Template(collection_prompts['user']).substitute(ctext=full_str)
            built_prompts[collection].append(user_prompt)
        # for item in built_prompts[collection]:
        #     print(f"built prompt: {item}\n")
    logger.info(f"Finished building prompt templates.")
    return built_prompts

def fit_prompt_templates_to_window(prompts, max_tokens):
    # INPUT: a dictionary that holds a list of prompts for each collection
    fitted_prompts = {}
    for collection, list_of_prompts in prompts.items():
        fitted_prompts[collection] = []
        for prompt in list_of_prompts:
            limited_prompt = llm_utils.fit_prompt_to_window(prompt, max_tokens)
            fitted_prompts[collection].append(limited_prompt)
            # print(f"prompt_fit_to_window:\n{limited_prompt}\n")
    logger.info(f"Finished fitting prompt templates to LLM token window limit {max_tokens}.")
    return fitted_prompts
            
def extract_entities_from_collections(collections, collection_prompts, doc_prompts, end_date, day_interval, limit, post_extraction_models, edge_notes_extraction_models, patch_extraction_models, llm_kwargs):
    kg_entities_relationships = {}
    start = timer()
    for collection in collections:
        if collection in doc_prompts:
            kg_entities_relationships[collection]=[]
            doc_prompt_templates = doc_prompts[collection]
            system_prompt = collection_prompts[collection]['system']
            if collection == "patch_management":
                model=patch_extraction_models['custom']
            elif collection in ["msrc_security_update", "windows_10", "windows_11", "windows_update_management"]:
                model=post_extraction_models['default']
            elif collection in ["stable_channel_notes", "security_update_notes", "mobile_stable_channel_notes", "beta_channel_notes"]:
                model=edge_notes_extraction_models['default']
            else:
                model=None
                print(f"Invalid collection type {collection}. Skipping.")

            for doc_prompt in tqdm(doc_prompt_templates, desc=f"LLM Completion Endpoint for Extraction {collection}"):
                # print(f"passing prompt to LLM: {doc_prompt}")
                
                llm_response = llm_utils.call_llm_completion(model, system_prompt, doc_prompt, llm_kwargs['max_tokens'], llm_kwargs['temperature'])
                # print(f"llm_response: {llm_response}\n")
                json_result = {}
                extracted_json = kg_extract_utils.extract_json_from_text(llm_response)
                if extracted_json:
                    try:
                        json_result = json.loads(extracted_json)
                        # print("successfully extracted json from llm response")
                    except json.JSONDecodeError as e:
                        # Handle any JSON parsing errors here
                        print(f"JSON parsing error: {e}\n{llm_response}\n----")
                    # check if there is an entity in the json_result with a label of Symptom. if it does, pass the value to a function 'clean_symptom_name' that removes the substrings ['symptom', 'Symptom', 'symptom_', 'Symptom']
                    if "entities" not in json_result:
                        logger.info(f"JSON object from LLM didn't contain 'entities' key: {json_result}")
                        continue       
                    # build the relationships from the entities
                    # here we are using the llm response to build the relationships
                    finalized_extractions_for_doc = kg_extract_utils.build_relationships_from_entities(json_result)
                    # print(f"finalized_extractions_for_doc: {finalized_extractions_for_doc}\n")
                    kg_entities_relationships[collection].append(finalized_extractions_for_doc)
        
    end = timer()
    logger.info(f"Entity extraction completed in {end-start} seconds.")
    
    return kg_entities_relationships

def build_training_data_for_kg_extraction(collections, doc_prompts, data, kg_entities):
    # doc_prompts is a dictionary of lists
    # data is a list of dataframe
    # extract_id_from_prompt
    # print("starting build_training_data_for_kg_extraction")
    # for collection, list in doc_prompts.items():
    #     print(f"collection: {collection}")
    #     print(f"list: {list}")
        
    # print(f"dataframe: {data}")
    run_collections = data.keys()
    for collection in run_collections:
        id_to_string = {}
        prompt_list = doc_prompts[collection]
        collection_df = data[collection]
        
        # deal with the prompts
        # print(f"cols in dataframe before: {collection_df.columns}")
        # print(f"num prompts for {collection}: {len(prompt_list)}")
        for prompt in prompt_list:
            id_ = kg_extract_utils.extract_id_from_prompt(prompt)
            if id_:
                id_to_string[id_] = prompt
        
        # Add a new column 'user_prompt' to the DataFrame
        collection_df['user_prompt'] = collection_df['id'].map(id_to_string)
        id_to_llm_response = {}
        # deal with the llm_responses
        for llm_response in kg_entities[collection]:
            # llm_response is a dict. it has a key 'entities' that is a list of dicts
            # print(f"llm_response: {llm_response}")
            # print(f"llm_response keys: {llm_response.keys()}")
            for entity in llm_response.get("entities", []):
                # print(f"entity: {entity}")
                # MAP JSON OBJECTS from json file to rows in dataframe based on mongo_id or id
                if entity.get("label") == "MSRCSecurityUpdate":
                    id_ = entity.get("mongo_id")
                    # print(f"top-level id: {id_}")
                    if id_:
                        id_to_llm_response[id_] = json.dumps(llm_response)
                        # print(f"storing llm_response in mapping dict: {llm_response}")
                elif entity.get("label") == "PatchManagement":
                    id_ = entity.get("id")
                    # print(f"top-level id: {id_}")
                    if id_:
                        id_to_llm_response[id_] = json.dumps(llm_response)
                        # print(f"storing llm_response in mapping dict: {llm_response}")
        collection_df['llm_response'] = collection_df['id'].map(id_to_llm_response)
        data[collection] = collection_df
        # print(f"cols in dataframe after: {collection_df.columns}")
        
    return tuple(data.get(collection, pd.DataFrame()) for collection in collections)


def save_entities_relationships(kg_entities):
    return kg_entities

def generate_cypher(collections, kg_entities_relationships):
    
    valid_entities = ["PatchManagement", "MSRCSecurityUpdate", "AffectedProduct", "Symptom", "Cause", "Fix", "Tool", "Feature"]
    e_statements = []
    r_statements = []
    
    run_collections = kg_entities_relationships.keys()
    for collection in run_collections:
        print(f"Generating cyphers for {collection}")
        
        for json_obj in tqdm(kg_entities_relationships[collection], desc="Generating cyphers for collection"):
            # Your existing code here

            e_label_map = {}

            # loop through the list of entities in our json object
            for i, entity_dict in enumerate(json_obj['entities']):
                # print(f"Generating cypher for entity {i+1} of {len(json_obj['entities'])} in collection: {collection}")
                # print(f"what is entity_dict: {entity_dict.keys()}\n{entity_dict.values()}.")
                
                # check if entity.label is in valid_entities, if the label is not in valid_entities, skip it
                
                if entity_dict.get("label"):
                    label = entity_dict["label"]
                    label = label.replace("-", "").replace("_", "")
                else:
                    label = "No label"
                
                # print(f"Is label valid? {label}")
                if label not in valid_entities:
                    print(f"rejecting entity with label: {entity_dict['label']}. skipping cypher statement.")
                    continue
                
                if label == "MSRCSecurityUpdate":
                    entity_id = entity_dict.get("mongo_id", "Unknown")
                    cypher = f'MERGE (n:{label} {{mongo_id: "{entity_id}"}})'
                else:
                    entity_id = entity_dict.get("id", "Unknown")
                    cypher = f'MERGE (n:{label} {{id: "{entity_id}"}})'
                # print(f"Is id valid? {id}")
                if label=="PatchManagement":
                    # print("found patch management entity")
                    normalized_title = kg_extract_utils.normalize_title(entity_dict["title"])
                    finalized_title = kg_extract_utils.escape_for_neo4j(normalized_title)
                    entity_dict["title"]=finalized_title
                    published_on = entity_dict["receivedDateTime"]
                    impact="pending"
                    severity="pending"
                    resolved="pending"
                elif label=="MSRCSecurityUpdate":
                    # print("found security update entity")
                    published_on = entity_dict["published"]
                    impact=entity_dict.get("impactType", None)
                    severity=entity_dict.get("maxSeverity", None)
                    resolved=entity_dict.get("remediationLevel", None)
                
                if 'description' in entity_dict:
                    # print(f"found description: {entity_dict['description']}")
                    entity_dict['description'] = kg_extract_utils.escape_for_neo4j(entity_dict['description'])
                    # print(f"description after: {entity_dict['description']}")
                
                if 'conversation_link' in entity_dict:
                    # print(f"link before make safe: {entity_dict['conversation_link']}")
                    entity_dict['conversation_link'] = kg_extract_utils.escape_for_neo4j(entity_dict['conversation_link'])
                    # print(f"link after make safe: {entity_dict['conversation_link']}")
                
                if label=="MSRCSecurityUpdate":
                    properties = {k: v for k, v in entity_dict.items() if k not in ["label", "faqs", "mongo_id"]}
                else:
                    properties = {k: v for k, v in entity_dict.items() if k not in ["label", "id"]}
                
                cypher = f'MERGE (n:{label} {{id: "{entity_id}"}})'
                if label == "MSRCSecurityUpdate":
                    cypher = f'MERGE (n:{label} {{mongo_id: "{entity_id}"}})'
                    
                if properties:
                    props_str = ", ".join([f'n.{key} = "{val}"' for key, val in properties.items()])
                    cypher += f" SET {props_str}"
                else:
                    print(f"no properties found for entity {entity_dict}")
                
                e_statements.append(cypher)
                print(f"updating e_label_map key -> {entity_id} value -> {label}")
                e_label_map[entity_id] = label
                
                # Process "faqs" if present
                if "faqs" in entity_dict:
                    #print("found faqs")
                    for faq in entity_dict["faqs"]:
                        faq_question = kg_extract_utils.escape_cypher_string(faq["question"])
                        faq_answer = kg_extract_utils.escape_cypher_string(faq["answer"])
                        faq_name = "FAQ"  # or any logic to derive the name

                        # Create the FAQ node
                        faq_id = kg_extract_utils.generate_faq_id(id, faq_question)
                        
                        faq_cypher = f"""MERGE (n:FAQ {{id: "{faq_id}"}}) ON CREATE SET n.question = "{faq_question}", n.answer = "{faq_answer}", n.name = "{faq_name}" """
                        e_statements.append(faq_cypher)
                        
                        # Create a relationship between the SecurityUpdate and FAQ nodes
                        rel_cypher = f"""MERGE (a:{label} {{mongo_id: "{entity_id}"}}) MERGE (b:FAQ {{id: "{faq_id}"}}) MERGE (a)-[:HAS_FAQ]->(b)"""

                        r_statements.append(rel_cypher)
                    # print("added faq cypher statements")

                        
            if json_obj.get("relationships"):
                # remove all dict items from relationships list, leave string items
                json_obj["relationships"] = [item for item in json_obj["relationships"] if isinstance(item, str)]
                
                for rs in json_obj["relationships"]:
                    # relationships is a list of strings format: src_id|rs_type|tgt_id
                    
                    rs_parts = rs.split("|")
                    
                    if len(rs_parts) != 3:
                        print(f"Invalid relationship format: doc: {id} \n{rs}. Skipping.")
                        continue
                    src_id, rs_type, tgt_id = rs_parts
                    
                    src_label = e_label_map.get(src_id)
                    tgt_label = e_label_map.get(tgt_id)
                    
                    if src_label and tgt_label:
                        # Determine the correct primary key (id or mongo_id) for each label
                        src_key = 'mongo_id' if src_label == "MSRCSecurityUpdate" else 'id'
                        tgt_key = 'mongo_id' if tgt_label == "MSRCSecurityUpdate" else 'id'

                        # Modify Cypher statement to use the correct keys
                        cypher = f"""MERGE (a:{src_label} {{{src_key}: "{src_id}"}}) MERGE (b:{tgt_label} {{{tgt_key}: "{tgt_id}"}}) MERGE (a)-[r:{rs_type}]->(b)"""
                        if rs_type in ["HAS_SYMPTOM", "HAS_CAUSE", "HAS_FIX", "HAS_FEATURE", "REFERENCES", "FOLLOWED_BY"]:
                            cypher += f' SET r.published_on = "{published_on}"'
                        elif rs_type in ["AFFECTS_PRODUCT"]:
                            cypher += f' SET r.impact = "{impact}", r.severity = "{severity}", r.resolved = "{resolved}"'
                        else:
                            cypher += f' SET r.published_on = "{published_on}"'

                        r_statements.append(cypher)
                    else:
                        print(f"Skipping relationship creation for {src_id} -> {rs_type} -> {tgt_id}. One or both nodes not found in e_label_map")
            else:
                print(f"post {id} has no relationships key")
    return e_statements, r_statements

def save_cypher_statements_intermediate(e_statements, r_statements):
    return "\n".join(e_statements + r_statements)

def augment_cypher_statements_node(cypher_statements):
    # look for existing entities in graph db and add additional relationships
    # for PatchManagement that means linking the posts in a thread and finding other useful relationships
    # for MSRC posts that means linking to other other MSRC posts that are related
    id_mapping_manager = IdMappingManager()
    # print(f"starting to augment {len(e_statements)} entites and {len(r_statements)} rels \n")
    all_cypher_statements = cypher_statements.split("\n")
    # print(f"starting to augment {len(all_cypher_statements)} cypher statements\n")
    augmented_cypher_statements = []
    for statement in tqdm(all_cypher_statements, desc="Augmenting Cypher Statements"):
        # pass back a list of cypher statements 
        
        augmented_statement_list = kg_extract_utils.augment_cypher_statements(statement, id_mapping_manager)
        # print(f"type: {type(augmented_statement_list)}, len: {len(augmented_statement_list)}\ values:{augmented_statement_list}")
        augmented_cypher_statements.extend(augmented_statement_list)
    # print(f"total cypher statements after augmentation: {len(augmented_cypher_statements)}\n") 
    return "\n".join(augmented_cypher_statements)


def save_cypher_statements_primary(augmented_cypher_statements):
    return augmented_cypher_statements
    
    
def load_cypher_statements(augmented_cypher_statements):
    statements_to_load = augmented_cypher_statements.split("\n")
    
    if len(statements_to_load) == 1 and len(statements_to_load[0]) == 0:
        return False
    else:
        for statement in tqdm(statements_to_load, desc="Loading Cypher Statements into Graph Store"):
            
            success = kg_extract_utils.execute_cypher(statement)
            # add key to mongo document added_to_graph_store
            if not success:
                print(f"error inserting cypher statement: {statement}\n")
        
        return True
    
        
def update_docstore_metadata_graph(collections, data, load_result):
    if load_result:
        logger.info(f"Finished loading cypher statements into graph store.")
        run_collections = data.keys()
        for collection in run_collections:
            collection_df = data[collection]
            collection_ids = collection_df['id'].tolist()
            
            print(f"collection: {collection} extracted {len(collection_ids)} ids from mongo\n{collection_ids}\n")
            
            update_result = db_collection.update_many(
                {"metadata.id": {"$in": collection_ids}},  # Condition to match documents
                {"$set": {"metadata.added_to_graph_store": True}}  # Update operation
            )
            # print(f"Updated {update_result.modified_count} documents in the collection '{collection}'")
    else:
        logger.info(f"No cypher statements loaded into graph store.")
    logger.info(f"Finished updating docstore metadata.")
