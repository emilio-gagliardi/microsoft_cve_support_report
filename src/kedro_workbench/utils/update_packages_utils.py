import re
import time
from datetime import datetime
from selenium.common.exceptions import InvalidArgumentException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from kedro_workbench.utils.feed_utils import setup_selenium_browser
import logging
from bs4 import BeautifulSoup
import hashlib
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs

conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]
mongo_creds = credentials["mongo_atlas"]

logger = logging.getLogger(__name__)


def navigate_to_url(driver, url):

    try:
        driver.get(url)
        return True
    except InvalidArgumentException as e:
        print(f"Error accessing {url}: Invalid URL. Exception: {e}")
    except Exception as e:
        print(f"An unexpected error occurred with {url}: {e}")
    return False


def wait_for_results(driver, class_name, timeout=20):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CLASS_NAME, class_name))
        )
        return True
    except TimeoutException:
        print(
            f"Selenium timed out after {timeout} sec waiting for {class_name} to load."
        )
    except Exception as e:
        print(f"An unexpected error occurred with Selenium WebDriverWait: {e}")
    return False


def parse_package_details(link_text):
    pattern = re.compile(
        # Capture the date
        r"(?P<date>\d{4}-\d{2})\s+"
        # Capture the update type
        r"(?P<update_type>Dynamic Cumulative|Cumulative)\s+Update\s+for\s+"
        # Capture the product version
        r"Windows\s+(?P<product_version>\d+)"
        # Adjusted to capture versions like 21H2 or 1507
        r"(\s+Version\s+(?P<version>\d{2}H\d|\d+))?"
        # Intermediate text
        r"\s+for\s+"
        # Capture the architecture
        r"(?P<architecture>x64|x86|arm64|ARM64)-based\s+Systems\s+"
        # Capture the KB number
        r"\(KB(?P<kb_number>\d+)\)"
    )

    details = {
        "date": None,
        "update_type": None,
        "product_version": None,
        "version": None,
        "architecture": None,
        "kb_number": None,
    }

    match = pattern.search(link_text)
    if match:
        details.update(match.groupdict())

    return details


def process_matching_link(link, driver, original_window, downloadable_package_details):
    # print("Link matches the criterion. Extract additional data from modal window.")
    downloadable_dict = {}
    link.click()
    WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
    time.sleep(2)
    for window_handle in driver.window_handles:
        if window_handle != original_window:
            driver.switch_to.window(window_handle)
            break
    time.sleep(3)
    last_modified = ""
    try:
        last_modified_span = driver.find_element(By.ID, "ScopedViewHandler_date")
        last_modified = last_modified_span.text
        last_modified = datetime.strptime(last_modified, "%m/%d/%Y")
    except Exception as e:
        print(f"An unexpected error occurred with Selenium ScopedViewHandler_date: {e}")
        last_modified = None

    file_size = ""
    try:
        file_size_span = driver.find_element(By.ID, "ScopedViewHandler_size")
        file_size = file_size_span.text
    except Exception as e:
        print(f"An unexpected error occurred with Selenium ScopedViewHandler_size: {e}")
        file_size = None
    time.sleep(1)
    try:
        # Wait until the modal window is visible
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "installDetails"))
        )
    except TimeoutException:
        print("Timeout waiting for installDetails.")
        driver.close()
        time.sleep(3)
        driver.quit()
        return None
    except Exception as e:
        print(f"An unexpected error occurred with Selenium WebDriverWait: {e}")
        driver.close()
        time.sleep(3)
        driver.quit()
        return None
    # Click the "Install Resources" tab
    try:
        install_resources_tab = driver.find_element(By.ID, "installDetails")
        install_resources_tab.click()
    except Exception as e:
        print(f"An unexpected error occurred with Selenium installDetails: {e}")
        return None
    time.sleep(1)  # Consider using WebDriverWait here as well

    # Scrape text from the "Install Resources" tab
    install_box = driver.find_element(By.ID, "installBox")
    html_content = install_box.get_attribute("innerHTML")
    install_info_text = install_box.text

    # Access the details using the keys
    date = downloadable_package_details["date"]
    update_type = downloadable_package_details["update_type"]
    product_version = downloadable_package_details["product_version"]
    version = downloadable_package_details["version"]
    architecture = downloadable_package_details["architecture"]
    kb_number = downloadable_package_details["kb_number"]
    # Note. product_version is coming back None resulting in a product_name of "windows_None"
    downloadable_dict = {
        "product_name": f"windows_{product_version}",
        "product_version": version,
        "product_architecture": architecture,
        "update_type": update_type,
        "install_resources_text": install_info_text,
        "install_resources_html": html_content,
        "last_modified": last_modified,
        "file_size": file_size,
    }
    product_version = downloadable_package_details.get("product_version")
    if product_version is None or product_version == "None":
        print(f"link generating prodct_name windows_None: {link}")
        print(f"{downloadable_package_details}\n")
    # print("=== Finished extracting additional data from modal window. ===\n")
    return downloadable_dict


def matches_criterion(downloadable_package_details, criterion):
    # print(f"matching link details to criterion {criterion}")
    # print(f"downloadable_package_details: {downloadable_package_details}")
    # Adjust the comparison logic to account for different representations of product and architecture
    # product_matches = criterion['product'].endswith(downloadable_package_details.get('product_version', ''))
    product_version = downloadable_package_details.get("product_version", "") or ""
    product_matches = criterion["product"].endswith(product_version)
    if criterion.get("version") is None:
        # If criterion version is None, only match if the downloadable_package_details version is also None
        version_matches = downloadable_package_details.get("version") is None
    else:
        # Otherwise, perform a direct comparison
        version_matches = criterion["version"] == downloadable_package_details.get(
            "version", ""
        )

    # architecture_matches = criterion['architecture'].startswith(downloadable_package_details.get('architecture', ''))
    architecture = downloadable_package_details.get("architecture", "") or ""
    architecture_matches = criterion["architecture"].startswith(architecture)
    matches = product_matches and version_matches and architecture_matches

    return matches


def process_downloadable_package(link, driver, original_window, criterion):
    # A downloadable package is a single line item for one update package page.
    # Each update package can have 1-to-many downloadable packages for various product versions and architectures
    # Each criterion represents a single product+version+architecture combination
    # Each downloadable_dict contains the requested details from the modal window for each line item
    # Currently the Install instructions are the focus, but it also includes date and file size and some other details
    # print("Start processing downloadable package link")
    text = link.text

    downloadable_package_details = parse_package_details(text)
    downloadable_dict = {}
    # print(f"does this link match the criterion?\n{text}")
    if not matches_criterion(downloadable_package_details, criterion):
        # print("The link does not match the criterion. Skipping.")
        return None
    # print("=== criterion matched downloadable_package_details ===\n")
    # Processing the link if it matches the criterion
    downloadable_dict = process_matching_link(
        link, driver, original_window, downloadable_package_details
    )
    # print(f"{downloadable_dict}")
    time.sleep(2)
    driver.close()
    driver.switch_to.window(original_window)
    return downloadable_dict


def find_downloadable_packages_for_update_package(
    driver, original_window, search_criteria, css_selector
):
    # search_criteria is a list of dicts with keys product, version, architecture
    # for each criterion, check each link on the update package page to determine if it should be extracted and stored as part of the update_package
    # return a list of downloadable packages for the update package

    downloadable_packages = []
    for j, criterion in enumerate(search_criteria):
        # get potential downloadable package links
        # print(f"processing criterion {j}: {criterion}")
        links = driver.find_elements(By.CSS_SELECTOR, css_selector)
        for link in links:

            downloadable_dict = process_downloadable_package(
                link, driver, original_window, criterion
            )
            if downloadable_dict:
                downloadable_packages.append(downloadable_dict)
    logger.info(f"found {len(downloadable_packages)} downloadable packages at {link}")
    return downloadable_packages


def display_execution_time(start_time):
    elapsed_time = time.time() - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    logger.info(
        f"Augmenting update package data Execution time: {int(minutes)} minutes and {seconds:.2f} seconds."
    )


def fetch_existing_update_packages(update_package_docs):
    """
    Fetch existing packages from MongoDB and return a dictionary where the key is `package_url`
    and the value is the document from the database.
    """
    mongo_update_packages_conn = MongoDBDocs(
        mongo_db="report_docstore",
        mongo_collection="microsoft_update_packages",
        credentials={
            "username": mongo_creds["username"],
            "password": mongo_creds["password"],
        },
    )
    urls = set(
        doc["package_url"] for doc in update_package_docs if doc.get("package_url")
    )

    # Define the aggregation pipeline to group by 'package_url' and get the most recent document
    # The assumption is that the most recent update_package will contain the most likely correct downloadable_packages array
    pipeline = [
        {
            "$match": {
                "package_url": {"$in": list(urls)},
                "downloadable_packages": {"$exists": True},
            }
        },
        {"$sort": {"published": -1}},
        {"$group": {"_id": "$package_url", "most_recent_doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$most_recent_doc"}},
    ]

    cursor = mongo_update_packages_conn.collection.aggregate(pipeline)
    update_packages_cache = {doc["package_url"]: doc for doc in cursor}

    return update_packages_cache


def process_downloadable_package_additional_details(
    doc, search_criteria, headless=True
):

    source_url = doc.get("package_url")
    if not source_url:
        return []

    print(f"fetching downloadable package details from: {source_url}")

    driver = setup_selenium_browser(None, headless)
    if not navigate_to_url(driver, source_url):
        print("Selenium driver could not access package url. Return to node.")
        driver.close()
        time.sleep(3)
        driver.quit()
        return []

    if not wait_for_results(driver, "resultsbottomBorder", 10):
        print(
            "Selenium driver could not find td that holds downloadable packages. Return to node."
        )
        driver.close()
        time.sleep(3)
        driver.quit()
        return []

    original_window = driver.current_window_handle

    downloadable_packages = find_downloadable_packages_for_update_package(
        driver,
        original_window,
        search_criteria,
        "td.resultsbottomBorder a.contentTextItemSpacerNoBreakLink",
    )
    driver.quit()

    return downloadable_packages


def extract_html_and_update_downloadable_packages(doc):
    # Check if 'downloadable_packages' exists and is not empty

    if "downloadable_packages" in doc:
        for package in doc["downloadable_packages"]:
            # Get the 'install_resources_html' content
            if "restart_behavior" not in package:
                html_content = package.get("install_resources_html", "")
                # If there's HTML content, parse it and update the document
                if html_content:
                    soup = BeautifulSoup(html_content, "html.parser")

                    # Extract information from HTML and update the package object
                    package["restart_behavior"] = (
                        soup.find(id="ScopedViewHandler_rebootBehavior").get_text(
                            strip=True
                        )
                        if soup.find(id="ScopedViewHandler_rebootBehavior")
                        else "Unknown"
                    )
                    package["request_user_input"] = (
                        soup.find(id="ScopedViewHandler_userInput").get_text(strip=True)
                        if soup.find(id="ScopedViewHandler_userInput")
                        else "Unknown"
                    )
                    package["installed_exclusively"] = (
                        soup.find(id="ScopedViewHandler_installationImpact").get_text(
                            strip=True
                        )
                        if soup.find(id="ScopedViewHandler_installationImpact")
                        else "Unknown"
                    )
                    package["requires_network"] = (
                        soup.find(id="ScopedViewHandler_connectivity").get_text(
                            strip=True
                        )
                        if soup.find(id="ScopedViewHandler_connectivity")
                        else "Unknown"
                    )

                    # Extract 'uninstall_notes' and remove the "Uninstall Notes:" prefix
                    uninstall_notes_text = (
                        soup.find(id="uninstallNotesDiv").get_text(strip=True)
                        if soup.find(id="uninstallNotesDiv")
                        else "Unknown"
                    )
                    # Replace only the first occurrence
                    package["uninstall_notes"] = uninstall_notes_text.replace(
                        "Uninstall Notes:", "", 1
                    )

                    uninstall_steps_text = (
                        soup.find(id="uninstallStepsDiv").get_text(strip=True)
                        if soup.find(id="uninstallStepsDiv")
                        else "Unknown"
                    )
                    # Replace only the first occurrence
                    package["uninstall_steps"] = uninstall_steps_text.replace(
                        "Uninstall Steps:", "", 1
                    )
    else:
        logger.info(
            f"WARNING. No 'downloadable_packages' found in Update Package with id {doc['id']}\n"
        )

    return doc


def generate_hash_from_dict(dict, keys_to_keep):
    # Create a string representation of the values based on keys to keep
    unique_string = "-".join(str(dict[key]) for key in keys_to_keep)
    # Generate a hash of the string
    return hashlib.md5(unique_string.encode()).hexdigest()
