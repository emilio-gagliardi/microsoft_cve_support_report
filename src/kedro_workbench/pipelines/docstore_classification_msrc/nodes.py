"""
This is a boilerplate pipeline 'docstore_classification_msrc'
generated using Kedro 0.18.11
"""
from llama_index.prompts import PromptTemplate
from kedro_workbench.utils.nlp_feature_engineering_prompts import (
    completion_post_type_classify_user_prompt, completion_post_type_classify_system_prompt
)
import kedro_workbench.utils.feature_engineering as feat_eng
from kedro_workbench.utils.llm_utils import classify_post, create_metadata_string_for_user_prompt, fit_prompt_to_window
from kedro_workbench.utils.json_utils import mongo_docs_to_dataframe
from kedro_workbench.utils.db_utils import remove_duplicates, print_mongo_result_properties
from kedro_workbench.utils.kedro_utils import convert_to_actual_type
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from pprint import pprint
import logging
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)

conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]
mongo_creds = credentials["mongo_atlas"]
parameters = conf_loader["parameters"]
post_types = parameters["msrc_classifications"]

# document_limit = parameters.get("document_limit", None)

mongo = MongoDBDocs(
    mongo_db="report_docstore",
    mongo_collection="docstore",
    credentials={
        "username": mongo_creds["username"],
        "password": mongo_creds["password"],
    },
)

def check_for_product_build_augment_complete(augmentation_complete):
    if not augmentation_complete:
        logger.warning("Product Build data augmentation didn't complete.")
        return False
    print(f"Proceed with feature engineering")
    return True

def extract_posts_to_classify(document_limit, begin_docstore_feature_engineering=True):
    if not begin_docstore_feature_engineering:
        logger.warning("Product Build data augmentation didn't complete.")
        
    limit = convert_to_actual_type(document_limit)
    # collections: list of collection names to extract posts from mongo docstore
    search_dict = {
        "$and": [
            {"metadata.collection": {"$in": ["msrc_security_update"]}},
            {"metadata.post_type": {"$exists": False}},
        ]
    }
    # print(search_dict)
    projection = {
        "metadata.id": 1,
        "metadata.post_id": 1,
        "metadata.revision": 1,
        'metadata.published': 1,
        "text": 1,
        "_id": 0,  # Optional: Exclude the default _id field if you don't need it
    }
    # print(projection)
    if limit is not None:
        result = mongo.find_docs(search_dict, projection)[:limit]
    else:
        result = mongo.find_docs(search_dict, projection)
    list_of_dicts_to_classify = list(result)
    logger.info(f"num posts to classify: {len(list_of_dicts_to_classify)}")
    
    # for item in list_of_dicts_to_classify:
    #     print(item['metadata']['post_id'])
    return list_of_dicts_to_classify if list_of_dicts_to_classify else ['None']

def transform_classification_data_msrc(docs, metadata_keys=None):
    if len(docs) == 1 and docs[0] == 'None':
        columns = ['id', 'post_id', 'published', 'text', 'revision', 'metadata_context', 'user_prompt', 'post_type']
        df = pd.DataFrame(columns=columns)
        return df
    # docs: list of dicts from mongo
    result_df = mongo_docs_to_dataframe(docs, metadata_keys=metadata_keys)
    logger.info(f"transformed list of dicts.")
    return result_df

def build_user_prompt_data_msrc(data, metadata_keys=None):
    if data.empty:
        return data
    data['metadata_context'] = data.apply(lambda row: create_metadata_string_for_user_prompt(row, metadata_keys), axis=1)
    logger.info(f"msrc classificaiton prompts built.")
    # print(f"new column added by build user prompt\n{data}")
    return data

def fit_classification_prompt_msrc(data, max_prompt_tokens):
    if data.empty:
        return data
    collection_label = "msrc_security_update"
    user_prompt_instructions = completion_post_type_classify_user_prompt.get(collection_label)
    separator = "------------------------\n"
    data['user_prompt'] = data.apply(lambda row: fit_prompt_to_window(user_prompt_instructions + separator + row['metadata_context'] + row['text'] + separator, max_prompt_tokens), axis=1)
    logger.info(f"User prompt fit to model token limit.")
    # print(f"new column added by fit_classification_prompt\n")
    # for index, row in data.iterrows():
    #     print(f"{row['user_prompt']}\n")
    return data
    
def classify_post_msrc_node(model, data, max_tokens, temperature):
    if data.empty:
        return data
    logger.info(f"using model: {model} to classify msrc posts")
    
    if not data.empty:
        collection_label = "msrc_security_update"
        source_column_name = 'classification_payload'  # The column containing JSON strings
        key_to_extract = 'classification'
        # Select system prompt and post types based on collection_label
        system_prompt = completion_post_type_classify_system_prompt.get(collection_label)
        
        # Apply classify_post function to each row
        tqdm.pandas(desc="Classifying msrc posts")
        data[source_column_name] = data.progress_apply(lambda row: classify_post(model, system_prompt, row['user_prompt'], max_tokens, temperature), axis=1)
        data['post_type'] = data.apply(feat_eng.extract_post_type, axis=1, args=(source_column_name, key_to_extract))

        logger.info(f"{data.shape[0]} msrc posts classified.")
        return data[['id', source_column_name, 'post_type']]
    else:
        raise ValueError("Classify_msrc_posts_node requires a non empty DataFrame as input.")

def batch_update_post_types_msrc(data):
    if data.empty:
        print(f"batch_update_classifications msrc: No records to process")
        return False
    else:
        num_matched = 0
        num_affected = 0
        for index, row in data.iterrows():
            record_id = row['id']
            new_post_type = row['post_type']
            # print(f"{index} - {record_id} - {new_post_type}")
            update_data = {
                'metadata.post_type': new_post_type,
                'metadata.added_to_graph_store': False
            }

            update_result = mongo.collection.update_one({'metadata.id': record_id}, {'$set': update_data}, upsert=True)
            # print_mongo_result_properties(update_result, verbose=False)
            num_matched += update_result.matched_count
            num_affected += update_result.modified_count
    
        logger.info(f"msrc post_type Mongo load complete. {num_matched} records matched and {num_affected} records affected.")
        
        return True
        
def remove_mongo_duplicates_msrc(update_flag):
    if update_flag:
        cursor = mongo.collection.aggregate([
            {
                "$match": {
                "metadata.collection": "msrc_security_update"
                }
            },
            {
                "$group": {
                "_id": {
                    "collection": "$metadata.collection",
                    "id": "$metadata.id"
                },
                "count": { "$sum": 1 },
                "duplicates": { "$push": "$_id" }
                }
            },
            {
                "$match": {
                "count": { "$gt": 1 }
                }
            }
            ])
        dups_list = list(cursor)
        if len(dups_list) > 0:
            print(f"found {len(dups_list)} duplicates in docstore...")
            deletion_summary = remove_duplicates(mongo.collection, dups_list)
            print(f"Deletion Summary: {deletion_summary}")
    
    
    return True
    

def begin_classification_patch_pipeline_connector(msrc_classification_status):
    
    logger.info(f"Classifying MSRC status: {msrc_classification_status}")
    return True