"""
This is a boilerplate pipeline 'docstore_feature_engineering_pm'
generated using Kedro 0.18.11
"""

# from llama_index.prompts import PromptTemplate
from kedro_workbench.utils.nlp_feature_engineering_prompts import (
    completion_post_type_classify_system_prompt,
    completion_keyword_evaluation_system_prompt,
    completion_noun_chunk_evaluation_system_prompt,
    completion_post_type_classify_user_prompt,
)
import kedro_workbench.utils.feature_engineering as feat_eng
from kedro_workbench.utils.llm_utils import (
    evaluate_rake_keywords,
    evaluate_noun_chunks,
    classify_email,
    create_metadata_string_for_user_prompt,
    fit_prompt_to_window,
    get_prompt,
)
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro_workbench.utils.json_utils import mongo_docs_to_dataframe
from kedro_workbench.utils.db_utils import (
    print_mongo_result_properties,
    remove_duplicates,
)
from kedro_workbench.utils.kedro_utils import convert_to_actual_type
from kedro.config import ConfigLoader
from kedro.framework.project import settings

# from pprint import pprint
import logging
import pandas as pd
from tqdm import tqdm

# set kedro logging level to debug
# logging.getLogger("kedro").setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]
mongo_creds = credentials["mongo_atlas"]
parameters = conf_loader["parameters"]
post_types = parameters["patch_classifications"]

mongo = MongoDBDocs(
    mongo_db="report_docstore",
    mongo_collection="docstore",
    credentials={
        "username": mongo_creds["username"],
        "password": mongo_creds["password"],
    },
)


def check_for_classification_msrc_complete(msrc_classification_status):
    if not msrc_classification_status:
        logger.warning("MSRC classification didn't complete.")
        return False
    print("Proceed with feature engineering patch management emails.")
    return True


def extract_patch_managment_to_clean(
    document_limit, begin_docstore_feature_engineering=True
):
    if not begin_docstore_feature_engineering:
        logger.warning("MSRC classification didn't complete.")

    limit = convert_to_actual_type(document_limit)
    search_dict = {
        "$and": [
            {"metadata.collection": "patch_management"},
            {"metadata.post_type": {"$exists": False}},
        ]
    }
    projection = {
        "metadata.id": 1,
        "metadata.topic": 1,
        "metadata.receivedDateTime": 1,
        "text": 1,
        "_id": 0,
    }
    if limit is not None:
        result = mongo.find_docs(search_dict, projection)[:limit]
    else:
        result = mongo.find_docs(search_dict, projection)
    list_of_dicts_to_clean = list(result)
    logger.info(f"num emails to clean: {len(list_of_dicts_to_clean)}")

    return list_of_dicts_to_clean if list_of_dicts_to_clean else ["None"]


def transform_feature_engineering_data_patch(docs, metadata_keys=None):
    # TODO Move column name list to params yaml
    if len(docs) == 1 and docs[0] == "None":
        columns = [
            "id",
            "topic",
            "receivedDateTime",
            "email_text_clean",
            "unique_tokens",
            "noun_chunks",
            "normalized_tokens",
            "all_keywords",
            "filtered_keywords",
            "evaluated_keywords",
            "evaluated_noun_chunks",
            "user_prompt",
            "conversation_link",
            "email_text",
            "metadata_context",
            "post_type",
        ]
        df = pd.DataFrame(columns=columns)
        return df
    # print(metadata_keys)
    result_df = mongo_docs_to_dataframe(docs, metadata_keys=metadata_keys)

    # Rename columns
    result_df = result_df.rename(columns={"text": "email_text"})
    # print(result_df)
    logger.info("transformed list of dicts to dataframe.")
    return result_df


def clean_email_text(data):
    if data.empty:
        return data
    logger.info("cleaning email texts")
    df_processed = feat_eng.text_clean_pipeline(data, "email_text_clean")
    # pprint(df_processed)
    # print(f"clean email all cols -> {df_processed.columns}")
    return df_processed


def get_unique_token_count(data):
    if data.empty:
        return data
    data["unique_tokens"] = data["email_text_clean"].apply(
        lambda x: len(feat_eng.get_unique_tokens(x))
    )
    # pprint(data)
    # print(f"get num unique all cols -> {data.columns}")
    return data


def generate_noun_phrases(data):
    if data.empty:
        return data
    data["noun_chunks"] = data["email_text_clean"].apply(feat_eng.get_noun_phrases)
    # pprint(data)
    # print(f"gen nouns all cols -> {data.columns}")
    return data


def build_lemmatized_tokens(data):
    if data.empty:
        return data
    data["normalized_tokens"] = data["email_text_clean"].apply(
        feat_eng.get_lemmatized_tokens
    )

    return data


def generate_keywords(data):
    if data.empty:
        return data
    data["all_keywords"] = data["normalized_tokens"].apply(feat_eng.extract_keywords)
    # pprint(data[['email_text_clean','all_keywords']])
    # print(f"gen keywords all cols -> {data.columns}")
    return data


def filter_keywords(data):
    if data.empty:
        return data
    data["filtered_keywords"] = data["all_keywords"].apply(feat_eng.filter_tuples)
    # print(f"filter_keywords all cols -> {data.columns}")
    return data


def evaluate_keywords_node(model, data, max_tokens, temperature):
    # pass keywords and email tokens to llm to evaluate keywords
    # create context column in 'evaluate_rake_keywords'

    if data.empty:
        return data
    # print(data['all_keywords'])
    system_prompt = completion_keyword_evaluation_system_prompt["patch_management"]

    tqdm.pandas(desc="Evaluating Keywords")
    data["evaluated_keywords"] = data.progress_apply(
        lambda x: evaluate_rake_keywords(
            model, x, system_prompt, max_tokens, temperature
        ),
        axis=1,
    )
    logger.info("Completed evaluating keywords.")
    # print(f"evaluate_keywords all cols -> {data.columns}")
    # print(data[['filtered_keywords', 'evaluated_keywords']])
    return data


def evaluate_noun_chunks_node(model, data, max_tokens, temperature):
    # pass noun chunks and email tokens to llm to evaluate noun chunks
    # create context column in 'evaluate_noun_chunks'
    if data.empty:
        return data
    system_prompt = completion_noun_chunk_evaluation_system_prompt["patch_management"]

    tqdm.pandas(desc="Evaluating Noun chunks")
    data["evaluated_noun_chunks"] = data.progress_apply(
        lambda x: evaluate_noun_chunks(
            model, x, system_prompt, max_tokens, temperature
        ),
        axis=1,
    )
    logger.info("Completed evaluating noun chunks.")
    # print(f"evaluate_noun_chunks all cols -> {data.columns}")
    # pprint(data[['filtered_keywords', 'evaluated_noun_chunks']])
    return data


def build_user_prompt_data_patch(data, metadata_keys=None):
    if data.empty:
        return data
    data["metadata_context"] = data.apply(
        lambda row: create_metadata_string_for_user_prompt(row, metadata_keys), axis=1
    )
    # print(f"build_prompts all cols -> {data.columns}")
    logger.info(f"patch classificaiton prompts built.")

    # for index, row in data.iterrows():
    #     print(f"{row['metadata_context']}\n")
    return data


def fit_classification_prompt_patch(data, max_prompt_tokens):
    # combine context and email tokens and user_prompt template
    # trim tokens outside of LLM token threshold
    # create classification_context variable
    if data.empty:
        return data
    collection_label = "patch_management"
    # user_prompt_instructions = completion_post_type_classify_user_prompt.get(
    #     collection_label
    # )
    user_prompt_instructions = get_prompt(
        completion_post_type_classify_user_prompt, collection_label
    )
    separator = "------------------------\n"
    data["user_prompt"] = data.apply(
        lambda row: fit_prompt_to_window(
            user_prompt_instructions
            + separator
            + str(row["metadata_context"])
            + str(row["email_text_clean"])
            + separator,
            max_prompt_tokens,
        ),
        axis=1,
    )
    data["classification_context"] = data.apply(
        lambda row: (
            f"Email metadata:\n---\n{row['metadata_context']}\n---\nEmail text:\n---\n{row['email_text_clean']}"
        ),
        axis=1,
    )
    logger.info("User prompt fit to model token limit.")
    # print(f"new column added by fit_classification_prompt: classification_context\n")
    # for index, row in data.iterrows():
    #     print(f"{row['classification_context']}\n")
    return data


def classify_emails_node(model, data, max_tokens, temperature):
    if data.empty:
        return data
    logger.info(f"using model: {model} to classify emails")

    collection_label = "patch_management"
    source_column_name = "classification_payload"
    key_to_extract = "classification"
    system_prompt = completion_post_type_classify_system_prompt.get(collection_label)

    # update the following, pass context
    tqdm.pandas(desc="Classifying emails")
    data[source_column_name] = data.progress_apply(
        lambda row: classify_email(
            model,
            system_prompt,
            row["user_prompt"],
            max_tokens,
            temperature,
            row["classification_context"],
        ),
        axis=1,
    )
    data["post_type"] = data.apply(
        feat_eng.extract_post_type, axis=1, args=(source_column_name, key_to_extract)
    )
    # pprint(data[['email_text_clean', 'post_type']])
    print(
        f"num patch entities to verify {data.shape[0]}"
        # f"num patch entities to verify {data.shape[0]}\n{data[['post_type', 'evaluated_keywords', 'evaluated_noun_chunks',]].head()}"
    )
    return data[
        [
            "id",
            source_column_name,
            "post_type",
            "evaluated_keywords",
            "evaluated_noun_chunks",
            "unique_tokens",
            "user_prompt",
            "conversation_link",
            "email_text_clean",
            "email_text",
        ]
    ]


def batch_update_new_features_patch(data):
    if data.empty:
        print(f"batch_update_new_features: No records to process")
        return False
    else:
        # print(f"batch_update debugging cols -> {data.columns}")
        num_matched = 0
        num_affected = 0
        for index, row in data.iterrows():

            record_id = row["id"]
            # print(f"creating update dict:\n{row}\n")
            print(f"conversation_link -> {row['conversation_link']}")
            conversation_link = (
                str(row["conversation_link"])
                if not pd.isna(row["conversation_link"])
                else "N/A"
            )
            update_data = {
                "metadata.email_text_original": row["email_text"],
                "text": row["email_text_clean"],
                "metadata.unique_tokens": row["unique_tokens"],
                "metadata.conversation_link": conversation_link,
                "metadata.evaluated_keywords": row["evaluated_keywords"],
                "metadata.evaluated_noun_chunks": row["evaluated_noun_chunks"],
                "metadata.post_type": row["post_type"],
                "metadata.added_to_graph_store": False,
            }

            # Update the MongoDB record and create fields if they don't exist
            # print(f"updating doc metadata: {update_data}")
            update_result = mongo.collection.update_one(
                {"metadata.id": record_id}, {"$set": update_data}, upsert=True
            )
            print_mongo_result_properties(update_result, verbose=False)
            num_matched += update_result.matched_count
            num_affected += update_result.modified_count
        logger.info(
            f"Completed patch management feature engineering save to Mongo."
            f"{num_matched} records matched and {num_affected} records affected."
        )
        return True


def remove_mongo_duplicates_patch(update_flag):
    if update_flag:
        cursor = mongo.collection.aggregate(
            [
                {"$match": {"metadata.collection": "patch_management"}},
                {
                    "$group": {
                        "_id": {
                            "collection": "$metadata.collection",
                            "id": "$metadata.id",
                        },
                        "count": {"$sum": 1},
                        "duplicates": {"$push": "$_id"},
                    }
                },
                {"$match": {"count": {"$gt": 1}}},
            ]
        )
        dups_list = list(cursor)
        if len(dups_list) > 0:
            print(f"found {len(dups_list)} duplicates in docstore patch_management...")
            deletion_summary = remove_duplicates(mongo.collection, dups_list)
            # print(f"Deletion Summary: {deletion_summary}")
    mongo.client.close()
