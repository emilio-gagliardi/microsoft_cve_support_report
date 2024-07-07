"""
This is a boilerplate pipeline 'augment_msrc_product_build_data'
generated using Kedro 0.18.11
"""

from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro_workbench.utils.update_packages_utils import (
    process_downloadable_package_additional_details,
    display_execution_time,
    extract_html_and_update_downloadable_packages,
    generate_hash_from_dict,
    fetch_existing_update_packages,
)
from kedro.config import ConfigLoader
from kedro.framework.project import settings

from datetime import datetime, timedelta, timezone
import pandas as pd
import time
import logging

logger = logging.getLogger(__name__)

conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]
mongo_creds = credentials["mongo_atlas"]


def check_for_product_build_ingestion_complete(ingestion_complete):
    """
    Ensures the sequential execution of the `augment_msrc_product_build_data` pipeline
    after the `ingest_msrc_product_build_data` pipeline.

    The last node of the ingestion pipeline outputs a memory dataset, which serves as
    an input to this function.
    This function, `check_for_product_build_ingestion_complete`,
    then outputs a memory dataset that acts as an input for the first node in the
    `augment_msrc_product_build_data` pipeline, `extract_existing_cve_docs_to_augment`.

    Args:
        inputs (bool): A boolean flag indicating the presence of the input memory dataset.
        outputs (bool): A boolean flag indicating the presence of the output memory dataset.
    """
    print(f"product build ingestion complete: {ingestion_complete}")
    return ingestion_complete


def extract_existing_cve_docs_to_augment(day_interval, begin_augmenting=True):
    if not begin_augmenting:
        logger.warning("Product Build data ingestion didn't complete.")

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=day_interval)
    mongo_to_fetch_existing_cves = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="docstore",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )

    query = {
        "$and": [
            {"metadata.published": {"$gte": start_date, "$lte": end_date}},
            {"metadata.collection": "msrc_security_update"},
        ]
    }
    projection = {"_id": 0}
    result = mongo_to_fetch_existing_cves.collection.find(query, projection)
    list_of_dicts_to_augment = list(result)
    logger.info(f"Num msrc docs to augment: {len(list_of_dicts_to_augment)}")
    logger.debug(
        "Post_ids being augmented with additional product build data"
        f"{[{doc['metadata']['post_id']} for doc in list_of_dicts_to_augment]}\n"
    )
    return list_of_dicts_to_augment


def extract_existing_product_build_data(
    day_interval, products_to_keep, columns_to_extract, begin_augmenting=True
):
    if not begin_augmenting:
        logger.warning("Could not extract product build data from document store.")

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=day_interval + 60)
    mongo_to_fetch_existing_cves = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_product_builds",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    products_to_keep = [
        item for sublist in products_to_keep.values() for item in sublist
    ]

    pipeline = [
        {"$match": {"published": {"$gte": start_date, "$lte": end_date}}},
        {"$match": {"product": {"$in": products_to_keep}}},
        {"$project": {"_id": 0}},
    ]
    result = mongo_to_fetch_existing_cves.collection.aggregate(pipeline)
    list_of_product_builds = list(result)
    product_build_data_df = pd.DataFrame(list_of_product_builds)
    # for idx, row in product_build_data_df.iterrows():
    #     print(f"{row['cve_id']} - {row['build_number']} - {row['product']}")
    logger.info(
        f"Num product build records to augment with: {product_build_data_df.shape}"
    )

    return product_build_data_df[columns_to_extract]


def merge_product_build_data_with_msrc_docs(
    new_product_build_data, existing_docs_to_augment, columns_to_keep
):
    # new_product_build_data is the downloaded product build data extracted from xlsx
    # existing_docs_to_augment is the list of existing cve docs extracted
    # from mongo to augment
    if len(existing_docs_to_augment) == 0 or new_product_build_data.empty:
        logger.info("no product build data to merge with existing msrc docs")
        return []

    dev_limit = None
    for cve in existing_docs_to_augment[:dev_limit]:
        cve_id = cve.get("metadata", {}).get("post_id")

        # Filter DataFrame rows where 'cve_id' matches the current cve_id
        filtered_rows = new_product_build_data[
            new_product_build_data["cve_id"] == cve_id
        ]
        for column in columns_to_keep:
            if column not in filtered_rows.columns:
                filtered_rows[column] = None

        if filtered_rows.empty:
            # logger.info(f"No matching product build data for CVE: {cve_id}")
            product_build_ids = []
            build_numbers_lists = []
            kb_articles_list = []
            products = []
            impact_type_mode = None
            severity_type_mode = None
        else:
            # logger.info(f"Found matching rows for CVE: {cve_id}")
            # print(filtered_rows)
            # Now you can work with filtered_rows,
            # which is a DataFrame containing only relevant rows
            # Convert 'product_build_id' column to a list
            product_build_ids = filtered_rows["product_build_id"].tolist()

            # Convert 'build_number' column to a set of tuples
            build_number_tuples = {tuple(bn) for bn in filtered_rows["build_number"]}

            # Create a list of dictionaries for KB articles with conditional key formatting
            kb_articles_list = [
                {
                    (
                        "kb" + str(kb_id)
                        if not str(kb_id).startswith("kb")
                        else str(kb_id)
                    ): url
                }
                for kb_id, url in zip(
                    filtered_rows["kb_id"], filtered_rows["article_url"]
                )
            ]
            # print(f"kbs for this cve:\n{kb_articles_list}")
            # Generate unique product strings by iterating over filtered rows
            unique_product_strs = {
                f"{row['product_name'].replace('_', ' ')} {row['product_version']} {row['product_architecture']}"
                for _, row in filtered_rows.iterrows()
            }

            # Convert the set of unique product strings to a list
            products = list(unique_product_strs)

            # Sort build number tuples and convert them back into the desired format
            sorted_build_number_tuples = sorted(build_number_tuples)
            build_numbers_lists = [
                list(bn_tuple) for bn_tuple in sorted_build_number_tuples
            ]
            # print(f"impact_type: {filtered_rows['impact_type']}")
            # For 'impact_type'
            if filtered_rows["impact_type"].mode().empty:
                impact_type_mode = None
            else:
                impact_type_mode = filtered_rows["impact_type"].mode()[0]

            # print(f"severity_type: {filtered_rows['severity_type']}")
            # For 'severity_type'
            if filtered_rows["severity_type"].mode().empty:
                severity_type_mode = None
            else:
                severity_type_mode = filtered_rows["severity_type"].mode()[0]

        # Update the cve metadata
        if "metadata" not in cve:
            cve["metadata"] = {}

        cve["metadata"].update(
            {
                "product_build_ids": product_build_ids,
                "build_numbers": build_numbers_lists,
                "kb_articles": kb_articles_list,
                "products": products,
                "impact_type": impact_type_mode,
                "severity_type": severity_type_mode,
            }
        )
    # for item in existing_docs_to_augment[:dev_limit]:
    #     print(
    #         f"{item['metadata']['id']} - {item['metadata']['source']} - {item['metadata']['products']} - {item['metadata']['kb_articles']}\n"
    #     )

    return existing_docs_to_augment


# ------------------ Load augmented existing cve docs to mongo


def load_augmented_msrc_posts_product_build_docs(updated_msrc_docs):
    if len(updated_msrc_docs) == 0:
        logger.info("no augmented msrc docs to load to mongo")
        return False
    logger.info(f"Processing {len(updated_msrc_docs)} MSRC docs...")
    mongo_to_load_augmented_cves = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="docstore",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    for cve in updated_msrc_docs:
        # The cve_id is used to identify the documents to update
        cve_id = cve.get("metadata", {}).get("post_id")
        query = {"metadata.post_id": cve_id}

        # Retrieve all documents with the matching post_id
        existing_docs = mongo_to_load_augmented_cves.collection.find(query)

        for existing_doc in existing_docs:
            update_data = {}

            # Get existing product build ids
            # some CVEs are processed multiple times and we only want to add
            # new and unique ones
            existing_product_build_ids = set(
                existing_doc["metadata"].get("product_build_ids", [])
            )
            # product build ids added in this instance
            new_product_build_ids = set(cve["metadata"].get("product_build_ids", []))
            # use set operation to remove existing and add only new and distinct ids
            product_build_ids_to_add = list(
                new_product_build_ids - existing_product_build_ids
            )
            # The MongoDB $addToSet operator is used to ensure that only unique values
            # are added to the array. $each setdefault Retrieves the value for the
            # specified key if it exists. If the key does not exist, it inserts the
            # key with the specified default value and then returns that default value.
            # dict.get('key', 'default value') does not modify the dict
            if product_build_ids_to_add:
                update_data.setdefault("$addToSet", {}).setdefault(
                    "metadata.product_build_ids", {}
                ).setdefault("$each", []).extend(product_build_ids_to_add)

            # Handle 'build_numbers' (adjusted approach for handling lists)
            existing_build_numbers = {
                tuple(bn) for bn in existing_doc["metadata"].get("build_numbers", [])
            }
            new_build_numbers = {
                tuple(bn) for bn in cve["metadata"].get("build_numbers", [])
            }
            build_numbers_to_add_tuples = new_build_numbers - existing_build_numbers
            build_numbers_to_add = [list(bn) for bn in build_numbers_to_add_tuples]

            if build_numbers_to_add:
                update_data.setdefault("$addToSet", {}).setdefault(
                    "metadata.build_numbers", {}
                ).setdefault("$each", []).extend(build_numbers_to_add)

            # Handle 'products' (assuming these are now stored as strings)
            existing_products = set(existing_doc["metadata"].get("products", []))
            new_products = set(cve["metadata"].get("products", []))
            products_to_add = list(new_products - existing_products)

            if products_to_add:
                update_data.setdefault("$addToSet", {}).setdefault(
                    "metadata.products", {}
                ).setdefault("$each", []).extend(products_to_add)

            # Handle 'severity_type' and 'impact_type'
            severity_type = cve["metadata"].get("severity_type")
            impact_type = cve["metadata"].get("impact_type")
            # Directly set 'severity_type' and 'impact_type', allowing None values
            # add key '$set' to update_data dict if it does not exist, then add key 'metadata.severity_type' and 'metadata.impact_type' to the '$set' key
            update_data.setdefault("$set", {})["metadata.severity_type"] = severity_type
            update_data.setdefault("$set", {})["metadata.impact_type"] = impact_type

            # Perform the update on the current document
            if update_data:
                update_result = mongo_to_load_augmented_cves.collection.update_one(
                    {"id_": existing_doc["id_"]},
                    {
                        "$addToSet": update_data.get("$addToSet", {}),
                        "$set": update_data.get("$set", {}),
                    },
                )

                # Check if the update was successful
                if update_result.matched_count > 0:
                    if update_result.modified_count > 0:
                        # logger.info(f"Update successful for MSRC document with id_: {existing_doc['id_']}")
                        pass
                    else:
                        # logger.info(f"MSRC Document with id_: {existing_doc['id_']} matched but was not modified.")
                        pass
                else:
                    logger.info(
                        f"No MSRC document found with id_: {existing_doc['id_']} "
                        "to update."
                    )

    # Close the MongoDB client connection
    mongo_to_load_augmented_cves.client.close()

    return True


def extract_update_packages_for_augmenting(day_interval, begin_extracting=True):
    # augment the update package docs from the install details at the package_url
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=day_interval + 21)

    update_packages_db_conn = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_update_packages",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    collection = update_packages_db_conn.collection
    query = {
        "$and": [
            {
                "$or": [
                    {"downloadable_packages": {"$exists": False}},
                    {"downloadable_packages": {"$size": 0}},
                ]
            },
            {"published": {"$gte": start_date, "$lt": end_date}},
        ]
    }
    projection_fields = {
        "_id": 0,
    }

    update_package_docs = list(collection.find(query, projection=projection_fields))
    logger.info(
        f"Augmenting {len(update_package_docs)} update packages"
        " with install instructions"
    )
    # print(update_package_docs[0])
    update_packages_db_conn.client.close()

    # for up in update_package_docs:
    #     print(f"{up['id']}-{up['package_url']}")
    return update_package_docs


def augment_update_packages_additional_details(update_package_docs, search_criteria):
    """
    Processes each document in the given list by fetching package details
    from specified URLs.

    For each document, it attempts to access the 'package_url', and if successful,
    searches for downloadable package details matching the given search criteria.
    It updates the document with the list of downloadable packages found.

    Args:
    - update_package_docs: A list of document dictionaries, each containing at least
    an 'id' and a 'package_url'.
    - search_criteria: A list of dictionaries specifying the search criteria
    for package details.
    """

    start_time = time.time()
    # fetch() returns a filtered dictionary of package_urls for keys
    # We can't store an empty url key so we must handle empty package_urls
    # independently of the lookup table
    existing_update_packages = fetch_existing_update_packages(update_package_docs)
    # print(f"update package cache:\n{existing_update_packages}")
    sorted_list = sorted(update_package_docs, key=lambda x: x["package_url"])
    num_new_update_packages = 0

    for j, doc in enumerate(sorted_list):
        package_url = doc.get("package_url")
        # print(f"processing document id: {doc['id']}\n{package_url}")
        if not package_url:
            # Directly assign an empty list to 'downloadable_packages'
            # for empty package_urls
            doc["downloadable_packages"] = []
        elif package_url in existing_update_packages:
            # If the package_url is in the cache, copy the downloadable_packages
            cached_downloadable_packages = existing_update_packages[package_url].get(
                "downloadable_packages", []
            )
            doc["downloadable_packages"] = cached_downloadable_packages
            # print(
            #     f"{j}:doc assigned downloadable_packages from cache\n"
            #     f"{doc['downloadable_packages']}"
            # )
        else:
            # extract the additional details from the html field and
            # create new fields with the data
            # Set headless to True or False below to monitor correct product pattern usage
            # print(f"This is a new package_url: {doc['package_url']}")
            downloadable_packages = process_downloadable_package_additional_details(
                doc, search_criteria, True
            )

            downloadable_packages = [
                package
                for package in downloadable_packages
                if "product_name" in package
                and "none" not in package["product_name"].lower()
            ]

            # Generate a hash for each downloadable package to prevent duplicates
            # in the monitoring collection
            keys_to_keep = [
                "parent_id",
                "product_name",
                "product_version",
                "product_architecture",
                "update_type",
            ]

            # loop over each downloadable_package item
            for item in downloadable_packages:
                item["parent_id"] = doc["id"]
                item["hash"] = generate_hash_from_dict(item, keys_to_keep)

            doc["downloadable_packages"] = downloadable_packages
            # print(f"adding download packages:\n{downloadable_packages}")
            # update the cache with the new package_url
            existing_update_packages[package_url] = doc
            logger.info(f"Updated the package_url cache with:\n{doc['package_url']}")
            num_new_update_packages += 1

    logger.info(
        f"Augmented {len(sorted_list)} Update Packages."
        f"{num_new_update_packages} new Update Package(s) encountered this run.\n"
    )
    display_execution_time(start_time)
    # for doc in sorted_list:
    #     print(
    #         f"{doc['id']}-build id: {doc['product_build_id']}-{doc['package_url']}-{doc['downloadable_packages']}\n\n"
    #     )
    return sorted_list


def parse_restructure_installation_details(augmented_update_package_docs):
    """
    Selenium grabs the html content from the download page which is stored as-is
    this function extracts pieces of text from the html and generates new
    keys to add to the document that are surfaced in the report. Returns
    augmented documents for loading in db.
    - file_size
    - package_type
    inputs:
        List[dicts]
    outputs:
        List[dicts]
    """
    for update_package in augmented_update_package_docs:
        update_package = extract_html_and_update_downloadable_packages(update_package)
        # print(f"parsed\n{update_package}\n")
    logger.info("Parsed downloadable packages installation details into keys.")
    return augmented_update_package_docs


def load_augmented_update_packages_to_db(augmented_update_package_docs):
    """
    update the documents in the database with the new keys-values
    The documents already exist, this step updates the keys and values
    After load is complete, return a boolean memory dataset to control
    loading of next pipeline in sequence.
    inputs:
        List[dict]
    outputs:
        boolean
    """
    update_packages_db_conn = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_update_packages",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    collection = update_packages_db_conn.collection
    for doc in augmented_update_package_docs:
        query = {"id": doc["id"]}
        value = doc.get("downloadable_packages")
        update_values = {"$set": {"downloadable_packages": value}}

        result = collection.update_one(query, update_values)
        if result.matched_count > 0:
            logger.debug(f"Update package with id {doc['id']} updated successfully.")
    update_packages_db_conn.client.close()
    logger.info(
        f"Updated {len(augmented_update_package_docs)} update packages "
        "with additional details."
    )
    return True


def begin_feature_engineering_pipeline_connector(augment_load_status):
    """
    utility function that creates an edge to the first node function in the
    next pipeline to ensure that augmenting product build data
    occurs before the NLP feature engineering next.

    inputs:
        boolean
    outputs:
        boolean
    """
    logger.info(
        "\n=====================\nAugmenting product build data "
        "completed\n=====================\n"
    )
    return True
