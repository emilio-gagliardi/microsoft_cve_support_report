"""
This is a boilerplate pipeline 'ingest_product_build_cve_data'
generated using Kedro 0.18.11
"""
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro_workbench.utils.feed_utils import (wait_for_specific_file, setup_selenium_browser, split_product_name, generate_hash, add_row_guid, convert_to_tuple, preprocess_update_guide_product_build_data, compile_update_guide_products_from_build_data,dataframe_to_string, hash_dataframe)
from kedro_workbench.utils.kedro_utils import make_row_hash
# from kedro.io import AbstractDataSet
# from kedro.io import DataCatalog
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from kedro.framework.session import KedroSession
# from kedro.framework.startup import bootstrap_project
from kedro_workbench.utils.kedro_utils import convert_to_actual_type
import pandas as pd
from typing import Any, Dict
# from pathlib import Path
# import os
# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
# import hashlib
from datetime import (datetime, timezone)
import time
# import uuid
import logging
# import pymongo
# from pymongo import MongoClient
# from pymongo.errors import PyMongoError
from pymongo import errors
from pymongo.errors import DuplicateKeyError
import re

logger = logging.getLogger(__name__)
conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]
mongo_creds = credentials["mongo_atlas"]

def check_for_preprocessing_complete(preprocessing_complete):
    logger.info(f"preprocessing complete: {preprocessing_complete}")
    return preprocessing_complete

def download_edge_product_build_data(skip_download, params, headless=False, begin_ingestion=True):
    if not begin_ingestion:
        logger.info("Preprocessing did not complete correctly.")
    if convert_to_actual_type(skip_download):
        return {"download_complete": True}
    else:
        start_time = time.time()
        download_path = params['edge']["download_path"]
        source_url = params['edge']["source_url"]
        
        driver = setup_selenium_browser(download_path, headless=convert_to_actual_type(headless))
        driver.get(source_url)
        # log progress
        logger.info("Configured Selenium browser to download Edge product build data")
        time.sleep(3)
        logger.info("Clear any active selections in the table for edge data")
        try:
            # Wait for the button to be clickable
            clear_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//span[contains(text(), 'Clear')]]"))
            )
            # Click the button if it is clickable (visible and not disabled)
            clear_button.click()
        except TimeoutException:
            print("Timeout waiting for the 'Clear' button to be clickable.")
        
        # edit columns in case 'Impact' and 'Max Severity' are not checked
        edit_columns_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Edit columns']]")))
        edit_columns_button.click()
        time.sleep(1)
        # Wait for the checkbox label to be present
        impact_label = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//span[text()='Impact']/ancestor::label")))

        # Get the outer div of the checkbox
        impact_outer_div = driver.find_element(By.XPATH, "//span[text()='Impact']/ancestor::div[contains(@class, 'ms-Checkbox')]")

        # Check if the checkbox is already selected by examining the class of the outer div
        is_checked = 'is-checked' in impact_outer_div.get_attribute("class")

        if not is_checked:
            # If not selected, click the label
            impact_label.click()
            # time.sleep(4)
        # Wait for the checkbox label to be present
        max_severity_label = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//span[text()='Max Severity']/ancestor::label")))

        # Get the outer div of the checkbox
        max_severity_outer_div = driver.find_element(By.XPATH, "//span[text()='Max Severity']/ancestor::div[contains(@class, 'ms-Checkbox')]")

        # Check if the checkbox is already selected by examining the class of the outer div
        is_checked = 'is-checked' in max_severity_outer_div.get_attribute("class")

        if not is_checked:
            # If not selected, click the label
            max_severity_label.click()
            # time.sleep(4)
        # close Edit Columns flyout
        close_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Close']]")))
        close_button.click()
        # time.sleep(4)
        # product family
        wait = WebDriverWait(driver, 10)
        product_family_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product Family']")))
        product_family_button.click()
        time.sleep(1)
        # product
        browser_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Browser'] and @role='menuitemcheckbox']")))
        browser_option_button.click()
        time.sleep(4)
        product_family_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product Family']")))
        product_family_button.click()
        time.sleep(1)
        product_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product']")))
        product_button.click()
        # time.sleep(4)
        edge_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Microsoft Edge (Chromium-based)'] and @role='menuitemcheckbox']")))
        edge_option_button.click()
        # time.sleep(4)
        product_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product']")))
        product_button.click()
        # time.sleep(4)
        # Initiate the download, open the flyout menu
        download_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Download']]")))
        download_button.click()
        # time.sleep(3)
        
        # start downloading the xlsx file
        start_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Start']]")))
        start_button.click()
        
        # File sizes vary, wait for the download to complete before proceeding.
        if wait_for_specific_file(download_path, expected_extension=".xlsx"):
            logger.info("Download completed successfully. New Edge product build data is available.")
            node_status = {"download_complete": True}
        else:
            logger.info("Download did not complete within the expected timeframe.")
            node_status = {"download_complete": False}
        driver.close()
        time.sleep(3)
        driver.quit()
        total_time = time.time() - start_time
        logger.info(f"Downloaded Edge product build data as xlsx. Total execution time: {total_time:.2f} seconds.")
        
    return node_status


def download_windows10_product_build_data(skip_download, params, headless=False, begin_ingestion=True):
    if not begin_ingestion:
        logger.info("Preprocessing did not complete correctly.")
    if convert_to_actual_type(skip_download):
        return {"download_complete": True}
    else:
        start_time = time.time()
        download_path = params['windows10']["download_path"]
        source_url = params['windows10']["source_url"]
        driver = setup_selenium_browser(download_path, headless=convert_to_actual_type(headless))
        driver.get(source_url)
        # log progress
        logger.info("Configured Selenium browser to download Windows 10 product build data")
        time.sleep(2)
        logger.info("Clear any active selections in the windows 10 table")
        try:
            # Wait for the button to be clickable
            clear_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//span[contains(text(), 'Clear')]]"))
            )
            # Click the button if it is clickable (visible and not disabled)
            clear_button.click()
        except TimeoutException:
            print("Timeout waiting for the 'Clear' button to be clickable.")
        # edit columns in case 'Impact' and 'Max Severity' are not checked
        edit_columns_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Edit columns']]")))
        edit_columns_button.click()
        # time.sleep(4)
        # method a
        # Wait for the checkbox label to be present
        impact_label = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//span[text()='Impact']/ancestor::label")))

        # # Get the outer div of the checkbox
        impact_outer_div = driver.find_element(By.XPATH, "//span[text()='Impact']/ancestor::div[contains(@class, 'ms-Checkbox')]")
        time.sleep(4)
        # # Check if the checkbox is already selected by examining the class of the outer div
        is_checked = 'is-checked' in impact_outer_div.get_attribute("class")
        
        if not is_checked:
            impact_label.click()
        # time.sleep(4)
        # Wait for the checkbox label to be present
        max_severity_label = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//span[text()='Max Severity']/ancestor::label")))

        # Get the outer div of the checkbox
        max_severity_outer_div = driver.find_element(By.XPATH, "//span[text()='Max Severity']/ancestor::div[contains(@class, 'ms-Checkbox')]")

        # Check if the checkbox is already selected by examining the class of the outer div
        is_checked = 'is-checked' in max_severity_outer_div.get_attribute("class")

        if not is_checked:
            # If not selected, click the label
            max_severity_label.click()
            # time.sleep(4)
        # close Edit Columns flyout
        close_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Close']]")))
        close_button.click()
        # end edit columns check
        time.sleep(1)
        # clear any previous selections
        clear_button = driver.find_elements(By.XPATH, "//button[.//i[@data-icon-name='Clear']]")
        time.sleep(1)
        # Check if the button is found and if it is not disabled
        if clear_button and not clear_button[0].get_attribute("disabled"):
            clear_button = clear_button[0]
            clear_button.click()

        time.sleep(1)
        wait = WebDriverWait(driver, 10)
        product_family_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product Family']")))
        product_family_button.click()
        # time.sleep(4)
        windows_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows']]")))
        windows_option_button.click()
        time.sleep(3)
        product_family_button.click()
        product_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product']")))
        product_button.click()
        time.sleep(1)
        # Select specific products from dropdown list
        #  Windows 10 product 
        windows_10_32bit_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 10 for 32-bit Systems']]")))
        windows_10_32bit_option_button.click()
        # time.sleep(4)
        #  Windows 10 product 
        windows_10_x64_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 10 for x64-based Systems']]")))
        windows_10_x64_option_button.click()
        # time.sleep(4)
        #  Windows 10 product 
        windows_10_22H2_32bit_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 10 Version 22H2 for 32-bit Systems']]")))
        windows_10_22H2_32bit_option_button.click()
        # time.sleep(4)
        #  Windows 10 product 
        windows_10_22H2_x64_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 10 Version 22H2 for x64-based Systems']]")))
        windows_10_22H2_x64_option_button.click()
        # time.sleep(4)
        #  Windows 10 product 
        windows_10_21H2_x64_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 10 Version 21H2 for x64-based Systems']]")))
        windows_10_21H2_x64_button.click()
        # time.sleep(4)
        #  Windows 10 product 
        windows_10_21H2_32bit_button_repeat = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 10 Version 21H2 for 32-bit Systems']]")))
        windows_10_21H2_32bit_button_repeat.click()
        # time.sleep(4)
        #  close dropdown list before proceeding 
        product_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product']")))
        product_button.click()
        # time.sleep(3)
        
        #  Initiate xlsx download, open flyout menu 
        download_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Download']]")))
        download_button.click()
        # time.sleep(3)
        
        # start the download
        start_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Start']]")))
        start_button.click()
        
        # Download time varies based on the number of records in the reporting period.
        # Ensure the file has completed downloading before proceeding.
        if wait_for_specific_file(download_path, expected_extension=".xlsx"):
            logger.info("Download completed successfully. New Windows 10 product build data is available.")
            node_status = {"download_complete": True}
        else:
            logger.info("Download did not complete within the expected timeframe.")
            node_status = {"download_complete": False}
        driver.close()
        time.sleep(3)
        driver.quit()
        total_time = time.time() - start_time
        logger.info(f"Downloaded Windows 10 product build data as xlsx. Total execution time: {total_time:.2f} seconds.")
    
    return node_status
    
    
def download_windows11_product_build_data(skip_download,params, headless=False, begin_ingestion=True):
    if not begin_ingestion:
        logger.info("Preprocessing did not complete correctly.")
    if convert_to_actual_type(skip_download):
        return {"download_complete": True}
    else:
        start_time = time.time()
        download_path = params['windows11']["download_path"]
        source_url = params['windows11']["source_url"]
        driver = setup_selenium_browser(download_path, headless=convert_to_actual_type(headless))
        driver.get(source_url)
        # log progress
        logger.info("Configured Selenium browser to download Windows 11 product build data")
        time.sleep(3)
        logger.info("Clear any active selections in the Windows 11 table")
        try:
            # Wait for the button to be clickable
            clear_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//button[.//span[contains(text(), 'Clear')]]"))
            )
            # Click the button if it is clickable (visible and not disabled)
            clear_button.click()
        except TimeoutException:
            print("Timeout waiting for the 'Clear' button to be clickable.")
        # edit columns in case 'Impact' and 'Max Severity' are not checked
        edit_columns_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Edit columns']]")))
        edit_columns_button.click()
        
        # Wait for the checkbox label to be present
        # time.sleep(4)
        
        impact_label = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//span[text()='Impact']/ancestor::label")))

        # Get the outer div of the checkbox
        impact_outer_div = driver.find_element(By.XPATH, "//span[text()='Impact']/ancestor::div[contains(@class, 'ms-Checkbox')]")

        # Check if the checkbox is already selected by examining the class of the outer div
        is_checked = 'is-checked' in impact_outer_div.get_attribute("class")

        if not is_checked:
            # If not selected, click the label
            impact_label.click()
        # time.sleep(4)
        
        # Wait for the checkbox label to be present
        max_severity_label = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//span[text()='Max Severity']/ancestor::label")))

        # Get the outer div of the checkbox
        max_severity_outer_div = driver.find_element(By.XPATH, "//span[text()='Max Severity']/ancestor::div[contains(@class, 'ms-Checkbox')]")

        # Check if the checkbox is already selected by examining the class of the outer div
        is_checked = 'is-checked' in max_severity_outer_div.get_attribute("class")

        if not is_checked:
            # If not selected, click the label
            max_severity_label.click()
            # time.sleep(4)
        # close Edit Columns flyout
        close_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Close']]")))
        close_button.click()
        # time.sleep(3)
        
        # select the product family
        wait = WebDriverWait(driver, 10)
        product_family_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product Family']")))
        product_family_button.click()
        # time.sleep(4)
        # Select the Windows option
        windows_option_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows']]")))
        windows_option_button.click()
        # time.sleep(3)
        
        # close the product family dropdown before proceeding
        product_family_button.click()
        
        # open the product dropdown
        product_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product']")))
        product_button.click()
        # time.sleep(4)
        # select windows 11 options
        # windows 11 product
        windows_11_21H2_x64_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 11 version 21H2 for x64-based Systems']]")))
        windows_11_21H2_x64_button.click()
        # time.sleep(4)
        # windows 11 product
        windows_11_22H2_x64_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 11 Version 22H2 for x64-based Systems']]")))
        windows_11_22H2_x64_button.click()
        # time.sleep(4)
        # windows 11 product
        windows_11_23H2_x64_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Windows 11 Version 23H2 for x64-based Systems']]")))
        windows_11_23H2_x64_button.click()
        # time.sleep(4)
        # close the product dropdown
        product_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@title='Product']")))
        product_button.click()
        # time.sleep(4)
        # initiate download, open the flyout menu
        download_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Download']]")))
        download_button.click()
        # time.sleep(4)
        # download the xlsx file
        start_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Start']]")))
        start_button.click()
        
        # file sizes vary, wait for the download to complete
        if wait_for_specific_file(download_path, expected_extension=".xlsx"):
            logger.info("Download completed successfully. New Windows 11 product build data is available.")
            node_status = {"download_complete": True}
        else:
            logger.info("Download did not complete within the expected timeframe.")
            node_status = {"download_complete": False}
        driver.close()
        time.sleep(3)
        driver.quit()
        total_time = time.time() - start_time
        logger.info(f"Downloaded Windows 11 product build data as xlsx. Total execution time: {total_time:.2f} seconds.")
    
    return node_status


# ---------------- Read xlsx files 

def extract_product_build_cve_windows_10_node(node_status, data, products_to_keep, overwrite=False):
    overwrite = convert_to_actual_type(overwrite)
    # print("Extract windows 10 node")
    # extract product build data from csv file
    # Create a hash for the entire dataframe
    downloaded_data_hash = hash_dataframe(data)
    mongo_for_file_check = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_product_build_source_files",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    query = {
        'hash': downloaded_data_hash
    }
    data['hash'] = data.apply(lambda row: make_row_hash(row), axis=1)
    
    escaped_products_to_keep = [re.escape(product) for product in products_to_keep]
    # print(f"products to keep: {products_to_keep}")
    regex_pattern = '|'.join(escaped_products_to_keep)
    # Check if the entire dataframe's hash exists in MongoDB
    if mongo_for_file_check.collection.find_one(query) is None or overwrite:
        logger.info("Processing new Windows 10 xlsx file. Processing all rows from dataframe.")
        properties = {
            "hash": downloaded_data_hash,
            "published": datetime.now().isoformat(),
        }
        if not overwrite:
            # Insert the properties of the new file into MongoDB
            result = mongo_for_file_check.collection.insert_one(properties)
            logger.info(f"[Product build - win10] Storing new file hash in MongoDB. Acknowledged:  {result.acknowledged}")
        else:
            print("Overwrite the product_build data in MongoDB.")
        
        # remove unwanted products from dataframe before proceeding
        # For some reason, Selenium code is not correctly selecting the correct products on each run
        # print(f"rows before filter: {data.shape[0]}")
        filtered_df = data[data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        inverse_filtered_df = data[~data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        if not inverse_filtered_df.empty:
            logger.info("WARNING: The download from Selenium contains out of bounds Products.")
        logger.info(f"[Product build - win10] shape: {filtered_df.shape}.")
        return filtered_df
    else:
        logger.info("Data already exists in MongoDB. Filtering rows from dataframe.")
        mongo_for_dedup_check = MongoDBDocs(
            mongo_db="report_docstore",
            mongo_collection="microsoft_product_builds",
            credentials={
                "username": mongo_creds["username"],
                "password": mongo_creds["password"],
            },
        )
        
        # Lists to store rows to keep and documents found in MongoDB
        data_to_keep_list = []
        
        # process only product_build data from xlsx file for specific subset of Products
        filtered_df = data[data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        # print(f"extract node: filtered_df\n {filtered_df.sample(n=12)}")
        inverse_filtered_df = data[~data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        if not inverse_filtered_df.empty:
            logger.info("WARNING: The download from Selenium contains out of bounds Products.")
        
        # Iterate over each row and check for its existence in MongoDB
        for index, row in filtered_df.iterrows():
            query = {'hash': row['hash']}
            existing_doc = mongo_for_dedup_check.collection.find_one(query)
            if not existing_doc:
                # If the document doesn't exist, add the row to the list to keep
                data_to_keep_list.append(index)
        
        data_to_keep_df = filtered_df.loc[data_to_keep_list].copy(deep=True)
        # print(data_to_keep_df.head())
        mongo_for_file_check.client.close()
        mongo_for_dedup_check.client.close()
        logger.info(f"[Product build - win10] num rows after cleanup: {data_to_keep_df.shape[0]}.")
        
        return data_to_keep_df


def extract_product_build_cve_windows_11_node(node_status, data, products_to_keep, overwrite=False):
    overwrite = convert_to_actual_type(overwrite)
    # print("Extract windows 11 node")
    # extract product build data from csv file
    downloaded_data_hash = hash_dataframe(data)
    mongo_for_file_check = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_product_build_source_files",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    query = {
        'hash': downloaded_data_hash
    }
    # print(f"products to keep: {products_to_keep}")
    escaped_products_to_keep = [re.escape(product) for product in products_to_keep]
    regex_pattern = '|'.join(escaped_products_to_keep)
    data['hash'] = data.apply(lambda row: make_row_hash(row), axis=1)
    
    if mongo_for_file_check.collection.find_one(query) is None or overwrite:
        logger.info("processing new Windows 11 xlsx file. processing all rows from dataframe.")
        properties = {
            "hash": downloaded_data_hash,
            "published": datetime.now().isoformat(),
        }
        if not overwrite:
            # Insert the properties of the new file into MongoDB
            result = mongo_for_file_check.collection.insert_one(properties)
            logger.info(f"[Product build - win11] Storing new file hash in MongoDB. Acknowledged: {result.acknowledged}")
        else:
            logger.info("Overwrite the product_build data in MongoDB.")
            
        filtered_df = data[data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        inverse_filtered_df = data[~data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        if not inverse_filtered_df.empty:
            logger.info("WARNING: The download from Selenium contains out of bounds Products.")
        logger.info(f"[Product build - win11] shape: {data.shape}.")
        # print(data.head())
        return filtered_df
    else:
        logger.info("Some Excel data already exists in MongoDB. Filtering rows from dataframe.")
        
        mongo_for_dedup_check = MongoDBDocs(
            mongo_db="report_docstore",
            mongo_collection="microsoft_product_builds",
            credentials={
                "username": mongo_creds["username"],
                "password": mongo_creds["password"],
            },
        )
        # Add a hash to each row of the dataframe
        
        data_to_keep_list = []
        filtered_df = data[data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        # print(f"extract node: filtered_df\n {filtered_df.sample(n=12)}")
        inverse_filtered_df = data[~data['Product'].str.contains(regex_pattern, regex=True, na=False)].copy(deep=True)
        if not inverse_filtered_df.empty:
            logger.info("WARNING: The download from Selenium contains out of bounds Products.")
        # print(f"[product build - win11 ] num rows before lookup: {data.shape[0]}")
        for index, row in filtered_df.iterrows():
            query = {'hash': row['hash']}
            existing_doc = mongo_for_dedup_check.collection.find_one(query)
            if not existing_doc:
                # If the document doesn't exist, add the index to the list to keep
                data_to_keep_list.append(index)
        
        data_to_keep_df = filtered_df.loc[data_to_keep_list]
        # print(data_to_keep_df.head())    
        logger.info(f"[Product build - win11] num rows after cleanup: {data_to_keep_df.shape[0]}.")
        
    return data_to_keep_df

def extract_product_build_cve_edge_node(node_status, data, overwrite=False):
    overwrite = convert_to_actual_type(overwrite)
    # extract product build data from csv file
    downloaded_data_hash = hash_dataframe(data)
    mongo_for_file_check = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_product_build_source_files",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    query = {
        'hash': downloaded_data_hash
    }
    data['hash'] = data.apply(lambda row: make_row_hash(row), axis=1)
    if mongo_for_file_check.collection.find_one(query) is None or overwrite:
        logger.info("Processing new Microsoft Edge xlsx file. processing all rows from dataframe.")
        properties = {
            "hash": downloaded_data_hash,
            "published": datetime.now().isoformat(),
        }
        if not overwrite:
            result = mongo_for_file_check.collection.insert_one(properties)
            logger.info(f"[Product build - edge] Storing new file hash in MongoDB. Acknowledged: {result.acknowledged}")
        
        logger.info(f"[Product build - edge] shape: {data.shape}.")
        
        return data
    else:
        logger.info("Some data from Excel already exists in MongoDB. Filtering rows from dataframe.")
        
        mongo_for_dedup_check = MongoDBDocs(
            mongo_db="report_docstore",
            mongo_collection="microsoft_product_builds",
            credentials={
                "username": mongo_creds["username"],
                "password": mongo_creds["password"],
            },
        )

        # Lists to store rows to keep and documents found in MongoDB
        data_to_keep_list = []
        
        # print(f"[product build - edge ] num rows before lookup: {data.shape[0]}")
        for index, row in data.iterrows():
            query = {'hash': row['hash']}
            existing_doc = mongo_for_dedup_check.collection.find_one(query)
            if not existing_doc:
                data_to_keep_list.append(index)
        
        data_to_keep_df = data.loc[data_to_keep_list]
        # print(data_to_keep_df.head())
            
        logger.info(f"[product build - edge] num rows after cleanup: {data_to_keep_df.shape[0]}.")
    return data

#  ---------------- Preprocess xlsx files
# Preprocessing cleans up the dataframe, renames columns, removes rows with missing build numbers and adjusts data types. It converts build numbers from strings to lists of ints and extracts pieces of the product name.
# -----------------------------------------
def preprocess_update_guide_product_build_data_windows10(params, data):
    
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 10 product build data to proceprocess.")
        return pd.DataFrame(columns=columns_to_keep)
    prepped_df = preprocess_update_guide_product_build_data(data, columns_to_keep)
    # print(f"preprocess data shape: {prepped_df.shape}, columns: {prepped_df.columns}")
    # print(prepped_df.sample(n=prepped_df.shape[0]))
    
    return prepped_df

def preprocess_update_guide_product_build_data_windows11(params, data):
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 11 product build data to proceprocess.")
        return pd.DataFrame(columns=columns_to_keep)
    prepped_df = preprocess_update_guide_product_build_data(data, columns_to_keep)
    # print(f"preprocess data shape: {prepped_df.shape}, columns: {prepped_df.columns}")
    # print(prepped_df.sample(n=prepped_df.shape[0]))
    
    return prepped_df

def preprocess_update_guide_product_build_data_edge(params, data):
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Edge product build data to proceprocess.")
        return pd.DataFrame(columns=columns_to_keep)
    prepped_df = preprocess_update_guide_product_build_data(data, columns_to_keep)
    # print(f"preprocess data shape: {prepped_df.shape}, columns: {prepped_df.columns}")
    # print(prepped_df.sample(n=prepped_df.shape[0]))
    
    return prepped_df

# ------------- Compile Products

def compile_update_guide_products_from_build_data_windows10(params, data):
    # create unique products from the product_build data
    # associate the CVEs and KB articles that reference the product
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 10 products to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    products_df = compile_update_guide_products_from_build_data(columns_to_keep, data)
    # print(products_df.columns)
    # print(products_df.head(n=5))
    
    return products_df

def compile_update_guide_products_from_build_data_windows11(params, data):
    # create unique products from the product_build data
    # associate the CVEs and KB articles that reference the product
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 11 products to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    products_df = compile_update_guide_products_from_build_data(columns_to_keep, data)
    # print(products_df.columns)
    # print(products_df.head(n=5))
    
    return products_df

def compile_update_guide_products_from_build_data_edge(params, data):
    # create unique products from the product_build data
    # associate the CVEs and KB articles that reference the product
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Edge products to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    products_df = compile_update_guide_products_from_build_data(columns_to_keep, data)
    # print(products_df.columns)
    # print(products_df.head(n=5))
    
    return products_df

# ------------- Compile KB Articles

def compile_update_guide_kb_article_data_windows10(params, data):
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 10 KB Article data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    kb_articles = data[columns_to_keep].copy()
    kb_articles = add_row_guid(kb_articles, columns_to_keep)
    kb_articles['build_number'] = kb_articles['build_number'].apply(convert_to_tuple)
    # unique_kb_articles = kb_articles.drop_duplicates(subset=['kb_id']).copy()
    kb_articles['summary'] = ""
    # print(kb_articles.columns)
    # with pd.option_context('display.max_colwidth', 400):
    #     print(kb_articles[['kb_id','product_build_id','article_url']].sample(n=kb_articles.shape[0]))
    
    return kb_articles
    

def compile_update_guide_kb_article_data_windows11(params, data):
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 11 KB Article data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    kb_articles = data[columns_to_keep].copy()
    kb_articles = add_row_guid(kb_articles, columns_to_keep)
    kb_articles['build_number'] = kb_articles['build_number'].apply(convert_to_tuple)
    # unique_kb_articles = kb_articles.drop_duplicates(subset=['kb_id']).copy()
    kb_articles['summary'] = ""
    # print(kb_articles.columns)
    # with pd.option_context('display.max_colwidth', 400):
    #     print(kb_articles[['kb_id','product_build_id','article_url']].sample(n=kb_articles.shape[0]))
    
    return kb_articles

def compile_update_guide_kb_article_data_edge(params, data):
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Edge KB Article data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    kb_articles = data[columns_to_keep].copy()
    kb_articles = add_row_guid(kb_articles, columns_to_keep)
    kb_articles['build_number'] = kb_articles['build_number'].apply(convert_to_tuple)
    # unique_kb_articles = kb_articles.drop_duplicates(subset=['kb_id']).copy()
    kb_articles['summary'] = ""
    # print(kb_articles.columns)
    # with pd.option_context('display.max_colwidth', 400):
    #     print(kb_articles[['kb_id','product_build_id','article_url']].sample(n=kb_articles.shape[0]))
    
    return kb_articles

# ------------------ Compile Product Builds

def compile_update_guide_build_data_windows10_node(params, data: pd.DataFrame) -> pd.DataFrame:
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 10 Product Build data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    product_builds_df = data[columns_to_keep].copy()
    data['build_number'] = data['build_number'].apply(convert_to_tuple)
    product_builds_df['summary'] = ""
    # print(product_builds_df.columns)
    # with pd.option_context('display.max_colwidth', 400):
    #     print(product_builds_df.sample(n=product_builds_df.shape[0]))
    
    return product_builds_df

def compile_update_guide_build_data_windows11_node(params, data: pd.DataFrame) -> pd.DataFrame:
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 11 Product Build data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    product_builds_df = data[columns_to_keep].copy()
    data['build_number'] = data['build_number'].apply(convert_to_tuple)
    product_builds_df['summary'] = ""
    # print(product_builds_df.columns)
    # with pd.option_context('display.max_colwidth', 400):
    #     print(product_builds_df.sample(n=product_builds_df.shape[0]))
        
    return product_builds_df

def compile_update_guide_build_data_edge_node(params, data: pd.DataFrame) -> pd.DataFrame:
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Edge Product Build data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    product_builds_df = data[columns_to_keep].copy()
    data['build_number'] = data['build_number'].apply(convert_to_tuple)
    product_builds_df['summary'] = ""
    # print(product_builds_df.columns)
    # with pd.option_context('display.max_colwidth', 400):
    #     print(product_builds_df.sample(n=product_builds_df.shape[0]))
        
    return product_builds_df

# ------------------ Compile Update Packages

def compile_update_guide_update_packages_windows10_node(params, data: pd.DataFrame ) -> pd.DataFrame:
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 10 Update Packages data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    update_packages = data[columns_to_keep].copy()
    update_packages_with_id = add_row_guid(update_packages, columns_to_keep)
    update_packages_with_id['summary'] = ""
    # print(f"update package source data win10:\n{update_packages_with_id.columns}\n{update_packages_with_id.shape}")
    # with pd.option_context('display.max_colwidth', 400):
    #     print(update_packages_with_id.sample(n=update_packages_with_id.shape[0]))
        
    return update_packages_with_id

def compile_update_guide_update_packages_windows11_node(params, data: pd.DataFrame ) -> pd.DataFrame:
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Windows 11 Update Packages data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    update_packages = data[columns_to_keep].copy()
    update_packages_with_id = add_row_guid(update_packages, columns_to_keep)
    update_packages_with_id['summary'] = ""
    # print(f"update package source data win11:\n{update_packages_with_id.columns}\n{update_packages_with_id.shape}")
    # with pd.option_context('display.max_colwidth', 400):
    #     print(update_packages_with_id.sample(n=update_packages_with_id.shape[0]))
        
    return update_packages_with_id

def compile_update_guide_update_packages_edge_node(params, data: pd.DataFrame ) -> pd.DataFrame:
    columns_to_keep = params["columns_to_keep"]
    if data.empty:
        logger.info("No Edge Update Packages data to compile.")
        return pd.DataFrame(columns=columns_to_keep)
    update_packages = data[columns_to_keep].copy()
    update_packages_with_id = add_row_guid(update_packages, columns_to_keep)
    update_packages_with_id['package_url'] = ""
    update_packages_with_id['summary'] = ""
    # print(f"update package source data edge:\n{update_packages_with_id.columns}\n{update_packages_with_id.shape}")
    # with pd.option_context('display.max_colwidth', 400):
    #     print(update_packages_with_id.sample(n=update_packages_with_id.shape[0]))
        
    return update_packages_with_id

# ------------------ Concatenate

def concatenate_update_guide_product_build_data(params, windows10, windows11, edge):
    columns_to_keep = params["columns_to_keep"]
    if windows10.empty and windows11.empty and edge.empty:
        logger.info("No Product Build data to concatenate. Returning empty dataframe.")
        return pd.DataFrame(columns=columns_to_keep)
    logger.info('Product Builds concat has data, concatenating...')
    concatenated_df = pd.concat([windows10, windows11, edge], ignore_index=True)
    # print(f"concatenated_df -product builds cols: {concatenated_df.columns}")
    correct_cols_df = concatenated_df[columns_to_keep]
    # with pd.option_context('display.max_colwidth', 400):
    #     print(correct_cols_df.sample(n=correct_cols_df.shape[0]))
        
    return correct_cols_df

def concatenate_update_guide_kb_article_data(params, windows10, windows11, edge):
    columns_to_keep = params["columns_to_keep"]
    if windows10.empty and windows11.empty and edge.empty:
        logger.info("No KB Article data to concatenate. Returning empty dataframe.")
        return pd.DataFrame(columns=columns_to_keep)
    logger.info('KB Articles concat has data, concatenating...')
    concatenated_df = pd.concat([windows10, windows11, edge], ignore_index=True)
    # print(f"concatenated_df -kb articles cols: {concatenated_df.columns}")
    correct_cols_df = concatenated_df[columns_to_keep]
    # with pd.option_context('display.max_colwidth', 400):
    #     print(correct_cols_df.sample(n=correct_cols_df.shape[0]))
        
    return correct_cols_df

def concatenate_update_guide_products_data(params, windows10, windows11, edge):
    columns_to_keep = params["columns_to_keep"]
    if windows10.empty and windows11.empty and edge.empty:
        logger.info("No Product data to concatenate. Returning empty dataframe.")
        return pd.DataFrame(columns=columns_to_keep)
    logger.info('Products concat has data, concatenating...')
    concatenated_df = pd.concat([windows10, windows11, edge], ignore_index=True)
    # print(f"concatenated_df -products cols: {concatenated_df.columns}")
    correct_cols_df = concatenated_df[columns_to_keep]
    
    # with pd.option_context('display.max_colwidth', None, 'display.max_columns', None):
    #     print(correct_cols_df.sample(n=correct_cols_df.shape[0]))
        
    return correct_cols_df

def concatenate_update_guide_update_packages_data(params, windows10, windows11, edge):
    columns_to_keep = params["columns_to_keep"]
    if windows10.empty and windows11.empty and edge.empty:
        logger.info("No Update Package data to concatenate. Returning empty dataframe.")
        return pd.DataFrame(columns=columns_to_keep)
    logger.info('Update Packages concat has data, concatenating...')
    concatenated_df = pd.concat([windows10, windows11, edge], ignore_index=True)
    
    correct_cols_df = concatenated_df[columns_to_keep]
    # print(f"concatenated_df - update packages shape: {correct_cols_df.shape}, cols: {correct_cols_df.columns}")
    # with pd.option_context('display.max_colwidth', 400):
    #     print(correct_cols_df.sample(n=correct_cols_df.shape[0]))
        
    return correct_cols_df

#  ------------------ Transform

# Insert product build data into mongo before augmenting cve docs
def transform_update_guide_product_build_data_to_list(data: pd.DataFrame) -> list:
    if data.empty:
        return [], True
    data['kb_id'] = data['kb_id'].apply(lambda x: 'kb' + str(x) if not str(x).startswith('kb') else str(x))
    # data['build_number'] = data['build_number'].apply(tuple)
    # unique_data = data.drop_duplicates(subset=['build_number', 'cve_id', 'kb_id']).copy()
    # unique_data['build_number'] = unique_data['build_number'].apply(list)
    # print(data.sample(n=3))
    return data.to_dict(orient="records"), True

def transform_update_guide_product_data_to_list(data: pd.DataFrame) -> list:
    if data.empty:
        return []
    # print(data.sample(n=3))
    return data.to_dict(orient="records")

def transform_update_guide_kb_articles_to_list(data: pd.DataFrame) -> list:
    if data.empty:
        return []
    data['build_number'] = data['build_number'].apply(list)
    # print(data.sample(n=3))
    return data.to_dict(orient="records")

def transform_update_guide_update_package_data_to_list(data: pd.DataFrame) -> list:
    if data.empty:
        return []
    data['build_number'] = data['build_number'].apply(list)
    # print(data.sample(n=3))
    return data.to_dict(orient="records")


# ------------------ Load to mongo

def load_update_guide_product_build_data(product_build_data, overwrite=False):
    """
    Insert or update product build records into MongoDB.
    
    :param product_build_data: List of dictionaries, each representing a product build record.
    :param overwrite: Boolean, if True existing records with the same hash will be overwritten.
    """
    logger.info("INGESTION: Loading Product Build data...")
    if product_build_data:
        logger.info(f"Num product build records to save: {len(product_build_data)}")
        mongo = MongoDBDocs(
            mongo_db="report_docstore",
            mongo_collection="microsoft_product_builds",
            credentials={
                "username": mongo_creds["username"],
                "password": mongo_creds["password"],
            },
        )
        num_modified = 0
        
        for record in product_build_data:
            record['published'] = datetime.strptime(record['published'], '%d-%m-%Y')
            query = {'hash': record['hash']}
            
            if overwrite:
                # Overwrite existing record
                result = mongo.collection.replace_one(query, record, upsert=True)
                if result.modified_count > 0:
                    num_modified += 1
            else:
                # Insert new record if hash not found
                if mongo.collection.find_one(query) is None:
                    try:
                        result = mongo.collection.insert_one(record)
                        if result.acknowledged:
                            num_modified += 1
                        else:
                            logger.info(f"Failed to insert record: {record}")
                    except ValueError as e:
                        logger.info("Error inserting document into MongoDB:", e)
                        logger.info("Document causing issue:", record)
        
        logger.info(f"INGESTION: Product Build data ingestion complete.\nNum product build records processed: {num_modified}")
    else:
        logger.info("INGESTION: No product build records to process.")


def load_update_guide_product_data(product_data):
    if len(product_data) > 0:
        
        mongo = MongoDBDocs(
            mongo_db="report_docstore",
            mongo_collection="microsoft_products",
            credentials={
                "username": mongo_creds["username"],
                "password": mongo_creds["password"],
            },
        )
        num_inserted = 0
        num_updated = 0
        for record in product_data:
            query = {
                'product_name': record['product_name'],
                'product_version': record['product_version'],
                'product_architecture': record['product_architecture']
            }
            # print(f"query: {query}")
            if mongo.collection.find_one(query) is None:
                record['published'] = datetime.combine(datetime.now().date(), datetime.min.time())
                try:
                    # Attempt to insert the record into MongoDB
                    result = mongo.collection.insert_one(record)
                    if result.acknowledged:
                        inserted_id = result.inserted_id
                        num_inserted += 1
                    else:
                        print(f"INGESTION: Product record failed to insert record: {record}")
                except errors.DuplicateKeyError as e:
                    # Catch DuplicateKeyError exceptions specifically
                    print("Duplicate document found, insertion skipped:", e)
                    continue
                except ValueError as e:
                    # Catch ValueError exceptions, which includes the NaTType utcoffset issue
                    print("ValueError inserting document into MongoDB:", e)
                    print("Document causing issue:", record)
                    continue
                except Exception as e:
                    # Catch any other exceptions that might occur
                    print("An unexpected error occurred while inserting document into MongoDB:", e)
                    print("Document causing issue:", record)
                    continue
            else:
                update_operations = {
                    '$addToSet': {
                        'build_number': {'$each': record['build_number']},
                        'cve_id': {'$each': record['cve_id']},
                        'kb_id': {'$each': record['kb_id']}
                    }
                }
                mongo.collection.update_one(query, update_operations)
                num_updated += 1
        logger.info(f"INGESTION: Num inserted products: {num_inserted}")
        logger.info(f"INGESTION: Num updated products: {num_updated}")
    else:
        logger.info(f"INGESTION: Num product records inserted: 0")

def load_update_guide_kb_article_data(kb_article_data, overwrite=False):
    """
    Insert or update KB article records into MongoDB.
    
    :param kb_article_data: List of dictionaries, each representing a KB article record.
    :param overwrite: Boolean, if True existing records with the same ID will be overwritten.
    """
    logger.info("INGESTION: Being loading kb articles...")
    if kb_article_data:
        logger.info(f"Num KB article records to save: {len(kb_article_data)}")
        mongo = MongoDBDocs(
            mongo_db="report_docstore",
            mongo_collection="microsoft_kb_articles",
            credentials={
                "username": mongo_creds["username"],
                "password": mongo_creds["password"],
            },
        )
        num_processed = 0  # This will count both inserted and updated records
        
        for record in kb_article_data:
            record['published'] = datetime.strptime(record['published'], '%d-%m-%Y')
            query = {'_id': record['id']}
            
            if overwrite:
                # Overwrite existing record
                result = mongo.collection.replace_one(query, record, upsert=True)
                if result.modified_count > 0 or result.upserted_id is not None:
                    num_processed += 1
            else:
                # Insert new record if ID not found
                if mongo.collection.find_one(query) is None:
                    try:
                        result = mongo.collection.insert_one(record)
                        if result.acknowledged:
                            num_processed += 1
                        else:
                            logger.info(f"Failed to insert record: {record}")
                    except ValueError as e:
                        logger.info("Error inserting document into MongoDB:", e)
                        logger.info("Document causing issue:", record)
        
        logger.info(f"INGESTION: Num loaded KB article records: {num_processed}")
    else:
        logger.info("INGESTION: No KB article records to process.")

def load_update_guide_update_package_data(update_package_data, overwrite=False):
    """
    Insert or update update package records into MongoDB.
    
    :param update_package_data: List of dictionaries, each representing an update package record.
    :param overwrite: Boolean, if True existing records with the same ID will be overwritten.
    """
    
    if update_package_data:
        print(f"Num update package records to save: {len(update_package_data)}")
        mongo = MongoDBDocs(
            mongo_db="report_docstore",
            mongo_collection="microsoft_update_packages",
            credentials={
                "username": mongo_creds["username"],
                "password": mongo_creds["password"],
            },
        )
        num_processed = 0  # This will count both inserted and updated records
        
        for record in update_package_data:
            record['published'] = datetime.strptime(record['published'], '%d-%m-%Y')
            query = {'_id': record['id'], 'product_build_id': record['product_build_id']}
            
            if overwrite:
                # Overwrite existing record
                result = mongo.collection.replace_one(query, record, upsert=True)
                if result.modified_count > 0 or result.upserted_id is not None:
                    num_processed += 1
            else:
                # Insert new record if ID not found
                if mongo.collection.find_one(query) is None:
                    try:
                        result = mongo.collection.insert_one(record)
                        if result.acknowledged:
                            num_processed += 1
                        else:
                            logger.info(f"INGESTION: Failed to insert Update Package: {record}")
                    except DuplicateKeyError:
                        logger.debug(f"INGESTION: Duplicate Update Package not inserted: {record}")
                    except ValueError as e:
                        logger.info(f"Error inserting Update Package into collection: {record} with Error\n{e}")
        
        logger.info(f"INGESTION: Num Update Package records: {num_processed} loaded.")
    else:
        logger.info("INGESTION: No Update Package records to process.")
    
    return True

        
def begin_augment_proudct_build_pipeline_connector(ingestion_complete):
    if ingestion_complete:
        logger.info("Ingest product build pipeline completed")
        return True
    return False
