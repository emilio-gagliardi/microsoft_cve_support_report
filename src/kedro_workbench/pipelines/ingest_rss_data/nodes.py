import hashlib
import logging
# import pprint
import time
from typing import Any, Dict, List
import re
from datetime import datetime
# import feedparser
import pandas as pd
# import requests
# from bs4 import BeautifulSoup
from kedro.config import ConfigLoader
from kedro.framework.project import settings
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.ui import WebDriverWait

from kedro_workbench.extras.datasets.ExcelDataSet import LatestExcelDataSet
from kedro_workbench.pipelines.ingest_product_build_cve_data.nodes import (
    download_edge_product_build_data,
    download_windows10_product_build_data,
    download_windows11_product_build_data,
)
from kedro_workbench.utils.feed_utils import (
    execute_js_on_element,
    extract_filter_product_build_data,
    setup_selenium_browser,
    wait_for_selenium_element,
)
# from kedro_workbench.utils.kedro_utils import make_row_hash

conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
all_params = conf_loader["parameters"]

logger = logging.getLogger(__name__)
logger_debug = logging.getLogger("DataSets")


def transform_published_parsed(d):
    if "published_parsed" in d:
        timestamp = d["published_parsed"]
        transformed_timestamp = {
            "tm_year": timestamp.tm_year,
            "tm_mon": timestamp.tm_mon,
            "tm_mday": timestamp.tm_mday,
            "tm_hour": timestamp.tm_hour,
            "tm_min": timestamp.tm_min,
            "tm_sec": timestamp.tm_sec,
            "tm_wday": timestamp.tm_wday,
            "tm_yday": timestamp.tm_yday,
            "tm_isdst": timestamp.tm_isdst,
        }
        d["published_parsed"] = transformed_timestamp
    return d


def extract_published_from_title(s):
    # Regex pattern to find date in the format "Month DD, YYYY"
    date_pattern = r'(\bJanuary|\bFebruary|\bMarch|\bApril|\bMay|\bJune|\bJuly|\bAugust|\bSeptember|\bOctober|\bNovember|\bDecember) \d{1,2}, \d{4}'

    # Try to find a date in the string
    match = re.search(date_pattern, s)
    if match:
        date_str = match.group()
        # Parse the date string into a datetime object
        # assuming the time as 12 midnight (00:00:00)
        date_obj = datetime.strptime(date_str, '%B %d, %Y')
        # Convert the datetime object to the required format with UTC timezone 'Z'
        formatted_date = date_obj.strftime('%a, %d %b %Y 00:00:00 Z')
        return formatted_date
    else:
        return None


def extract_rss_1_feed(feed) -> Dict[str, Any]:
    # no processing required
    # data is extracted from custom dataset and returned to the node
    return feed


def transform_rss_1_feed(
    raw_rss_feed: List[Dict[str, Any]], params: Dict[Any, Any], skip_download
) -> List[Dict[str, Any]]:

    entries = raw_rss_feed
    keys_to_keep = params["keys_to_keep"]
    keys_to_hash = params["keys_to_hash"]
    filtered_keys = [{k: d[k] for k in keys_to_keep if k in d} for d in entries]
    transformed_rss_feed = [transform_published_parsed(d) for d in filtered_keys]

    for item in transformed_rss_feed:

        if "link" in item.keys():
            item["source"] = item.pop("link")
            # print(f"{item.keys()}")
        subset_dict = {key: item[key] for key in keys_to_hash if key in item}
        dict_hash = hashlib.sha256(
            str(sorted(subset_dict.items())).encode()
        ).hexdigest()
        item["hash"] = dict_hash

    download_params = all_params['product_build_ingestion_params']
    product_patterns_windows_10 = (
        all_params['product_build_product_patterns']['windows_10']
    )
    product_patterns_windows_11 = (
        all_params['product_build_product_patterns']['windows_11']
    )
    product_patterns_edge = all_params['product_build_product_patterns']['edge']

    # downloading locations are set in the parameters.yml
    node_status = download_windows10_product_build_data(
        skip_download, download_params,
        headless=True,
        begin_ingestion=True
    )
    node_status = download_windows11_product_build_data(
        skip_download,
        download_params,
        headless=True,
        begin_ingestion=True
    )
    node_status = download_edge_product_build_data(
        skip_download,
        download_params,
        headless=True,
        begin_ingestion=True
    )
    logger.info(f"Download of product data by product complete.{node_status}")

    csv_data_windows_10 = LatestExcelDataSet(
        filepath="data/01_raw/windows_10/",
        load_args={"sheet_name": "Security Updates"}
    )
    csv_data_windows_11 = LatestExcelDataSet(
        filepath="data/01_raw/windows_11/",
        load_args={"sheet_name": "Security Updates"}
    )
    csv_data_edge = LatestExcelDataSet(
        filepath="data/01_raw/edge/",
        load_args={"sheet_name": "Security Updates"}
    )
    data_win10 = csv_data_windows_10.load()
    data_win11 = csv_data_windows_11.load()
    data_edge = csv_data_edge.load()

    windows_10_df = extract_filter_product_build_data(
        data_win10,
        product_patterns_windows_10
    )
    windows_10_df.drop_duplicates(
        subset='Details',
        keep='first',
        inplace=True
        )

    windows_11_df = extract_filter_product_build_data(
        data_win11,
        product_patterns_windows_11
    )
    windows_11_df.drop_duplicates(
        subset='Details',
        keep='first',
        inplace=True
        )

    edge_df = extract_filter_product_build_data(
        data_edge,
        product_patterns_edge
    )
    edge_df.drop_duplicates(
        subset='Details',
        keep='first',
        inplace=True
    )

    # verified correct output to this point
    concatenated_df = pd.concat(
        [windows_10_df,
         windows_11_df,
         edge_df
         ],
        ignore_index=True
    )
    concatenated_df.drop_duplicates(subset='Details', keep='first', inplace=True)

    columns_to_keep = ['Release date',
                       'Article',
                       'Article_URL',
                       'Details',
                       'Details_URL'
                       ]
    concatenated_df['Release date'] = pd.to_datetime(
        concatenated_df['Release date'],
        format='%b %d, %Y'
        )
    concatenated_df['Release date'] = (
        concatenated_df['Release date'].dt.strftime('%a, %d %b %Y 00:00:00 Z')
        )
    concatenated_df = concatenated_df[columns_to_keep].copy()

    concatenated_df.rename(
        columns={'Release date': 'published',
                 'Details': 'post_id',
                 'Details_URL': 'source',
                 'Article': 'kb_id',
                 'Article_URL': 'article_url'
                 },
        inplace=True)
    # for idx, row in concatenated_df.iterrows():
    #     print(f"{row}")

    driver = setup_selenium_browser(None, headless=True)
    driver.implicitly_wait(1)
    downloaded_cves = []
    collection = "msrc_security_update"
    category = "CVE"

    for idx, row in concatenated_df.iterrows():
        temp_dict = {}
        source_url = row['source']
        published = row['published']
        post_id = row['post_id']

        if not source_url:
            logger.info(
                f"RSS Ingestion found no source_url in concatenated_df[{post_id}]"
                )
            continue

        # print(f"fetching: {source_url}")
        driver.get(source_url)
        time.sleep(1)
        header_element = wait_for_selenium_element(
            driver,
            "h1.ms-fontWeight-semibold.css-196")
        # print(f"header_element {header_element}")
        header_text = (
            execute_js_on_element(
                driver, header_element,
                "return arguments[0].childNodes[0].nodeValue.trim();"
            ) if header_element else "MSRC Title not found"
        )

        revisions_grid = wait_for_selenium_element(
            driver,
            "div[role='grid'][aria-label='Revisions']"
            )
        if revisions_grid:
            # Locate the first row within the revisions grid
            first_row = wait_for_selenium_element(
                driver,
                "div.ms-List-cell[data-list-index='0']",
                parent=revisions_grid
                )
            if first_row:
                # Locate and extract text from the version element within the first row
                version_element = wait_for_selenium_element(
                    driver,
                    "div[data-automationid='DetailsRowCell'][data-automation-key='version']",
                    parent=first_row
                    )
                if version_element:
                    version_text = execute_js_on_element(
                        driver,
                        version_element,
                        "return arguments[0].innerText;"
                        )
                else:
                    version_text = "0.9"

                # Locate and extract text from the description element
                # within the first row
                description_element = wait_for_selenium_element(
                    driver,
                    "div[data-automationid='DetailsRowCell'][data-automation-key='description'] p",
                    parent=first_row
                    )
                if description_element:
                    description_text = execute_js_on_element(
                        driver,
                        description_element,
                        "return arguments[0].innerText;"
                        )
                else:
                    description_text = "Information published."
            else:
                version_text = "0.9"
                description_text = "Information published."
        else:
            version_text = "0.9"
            description_text = "Information published."

        version_padded = "{:.10f}".format(float(version_text))

        temp_dict = {
            'post_id': post_id,
            'collection': collection,
            'category': category,
            'source': source_url,
            'title': header_text,
            'revision': version_padded,
            'description': description_text,
            'published': published
        }
        temp_dict['hash'] = hashlib.sha256(
            str(sorted(temp_dict.items())).encode()
        ).hexdigest()
        # print(f"alternate ingestion found a title -> {temp_dict['title']}")
        downloaded_cves.append(temp_dict)

    # print("RSS CVEs")
    sorted_transformed_rss_feed = sorted(
        transformed_rss_feed,
        key=lambda x: x['post_id'],
        reverse=True
        )
    # for item in sorted_transformed_rss_feed:
    #     print(f"{item['post_id']} - {item['source']}")

    sorted_downloaded_cves = sorted(
        downloaded_cves,
        key=lambda x: x['post_id'],
        reverse=True
        )
    # print("EXCEL CVEs")
    # for item in sorted_downloaded_cves:
    #     print(f"{item['post_id']} - {item['source']}")

    for dict2 in sorted_downloaded_cves:
        if not any(
            dict1['post_id'] == dict2['post_id']
            for dict1 in sorted_transformed_rss_feed
        ):
            # print(f"adding EXCEL CVE to RSS CVE {dict2['post_id']}")
            sorted_transformed_rss_feed.append(dict2)
    driver.close()
    time.sleep(3)
    driver.quit()
    # for rss_1 in sorted_transformed_rss_feed:
    #     print(rss_1)
    logger.info(
        f"RSS Ingestion of MSRC posts found {len(transformed_rss_feed)} MSRC posts.\n"
        f"Excel Ingestion found {len(sorted_downloaded_cves)} MSRC posts."
    )
    logger.info(
        f"Final MSRC Ingestion total: {len(sorted_transformed_rss_feed)}.\n"
        "NOTE. RSS ingestion fetches all MSRC posts from RSS feed, so the total "
        "doesn't reflect how many posts will be inserted."
    )
    return sorted_transformed_rss_feed


def load_rss_1_feed(preprocessed_rss_feed):

    return preprocessed_rss_feed


# rss_2 is windows update
def extract_rss_2_feed(feed) -> Dict[str, Any]:
    # no processing required

    return feed


def transform_rss_2_feed(
    raw_rss_feed: List[Dict[str, Any]], params: Dict[Any, Any]
) -> List[Dict[str, Any]]:

    entries = raw_rss_feed
    keys_to_keep = params["keys_to_keep"]
    keys_to_hash = params["keys_to_hash"]
    filtered_keys = [{k: d[k] for k in keys_to_keep if k in d} for d in entries]
    transformed_rss_feed = [transform_published_parsed(d) for d in filtered_keys]

    for item in transformed_rss_feed:
        if "link" in item.keys():
            item["source"] = item.pop("link")
        subset_dict = {key: item[key] for key in keys_to_hash if key in item}
        dict_hash = hashlib.sha256(
            str(sorted(subset_dict.items())).encode()
        ).hexdigest()
        item["hash"] = dict_hash
    return transformed_rss_feed


def load_rss_2_feed(preprocessed_rss_feed):

    return preprocessed_rss_feed


# rss_3 is windows 10
def extract_rss_3_feed(feed) -> Dict[str, Any]:
    # no processing required

    return feed


def transform_rss_3_feed(
    raw_rss_feed: List[Dict[str, Any]], params: Dict[Any, Any]
) -> List[Dict[str, Any]]:

    entries = raw_rss_feed
    keys_to_keep = params["keys_to_keep"]
    keys_to_hash = params["keys_to_hash"]
    filtered_keys = [{k: d[k] for k in keys_to_keep if k in d} for d in entries]
    transformed_rss_feed = [transform_published_parsed(d) for d in filtered_keys]

    for item in transformed_rss_feed:
        if "link" in item.keys():
            item["source"] = item.pop("link")
        # check if there is a date in the title
        # if there is a date, convert it into raw format
        title_published_date = extract_published_from_title(item["title"])
        if title_published_date:
            item["published"] = title_published_date
        subset_dict = {key: item[key] for key in keys_to_hash if key in item}
        dict_hash = hashlib.sha256(
            str(sorted(subset_dict.items())).encode()
        ).hexdigest()
        item["hash"] = dict_hash
    # for item in transformed_rss_feed:
    #     print(item)
    return transformed_rss_feed


def load_rss_3_feed(preprocessed_rss_feed):

    return preprocessed_rss_feed


# rss_3 is windows 11
def extract_rss_4_feed(feed) -> Dict[str, Any]:
    # no processing required

    return feed


def transform_rss_4_feed(
    raw_rss_feed: List[Dict[str, Any]], params: Dict[Any, Any]
) -> List[Dict[str, Any]]:

    entries = raw_rss_feed
    keys_to_keep = params["keys_to_keep"]
    keys_to_hash = params["keys_to_hash"]
    filtered_keys = [{k: d[k] for k in keys_to_keep if k in d} for d in entries]
    transformed_rss_feed = [transform_published_parsed(d) for d in filtered_keys]
    for item in transformed_rss_feed:
        if "link" in item.keys():
            item["source"] = item.pop("link")
        title_published_date = extract_published_from_title(item["title"])
        if title_published_date:
            item["published"] = title_published_date
        subset_dict = {key: item[key] for key in keys_to_hash if key in item}
        dict_hash = hashlib.sha256(
            str(sorted(subset_dict.items())).encode()
        ).hexdigest()
        item["hash"] = dict_hash
    # for item in transformed_rss_feed:
    #     print(item)
    return transformed_rss_feed


def load_rss_4_feed(preprocessed_rss_feed):

    return preprocessed_rss_feed
