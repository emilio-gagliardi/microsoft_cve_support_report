from kedro_workbench.extras.datasets.MongoDataset import MongoDBDocs
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from kedro_workbench.utils.feed_utils import split_product_name
import pymongo
from pymongo import MongoClient
import re
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import pandas as pd
from pandas.tseries.offsets import DateOffset
import csv
import hashlib
import pytz
import logging

logger = logging.getLogger(__name__)
conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]
mongo_creds = credentials["mongo_atlas"]

def build_mongo_pipeline(start_date, end_date, products_list):
    logger.info(f"Building pipeline to fetch MSRC documents for start_date: {start_date}, end_date: {end_date}, products_list: {products_list}")
    return [
        {
            "$match": {
                "metadata.collection": "msrc_security_update",
                "metadata.published": {"$gte": start_date, "$lte": end_date},
                "metadata.build_numbers": {"$exists": True, "$ne": []},
                "metadata.products": {"$in": products_list}
            }
        },
        {"$addFields": {"metadata.revisionFloat": {"$convert": {"input": "$metadata.revision", "to": "double", "onError": 0.0, "onNull": 0.0}}}},
        {"$sort": {"metadata.post_id": 1, "metadata.revisionFloat": -1}},
        {"$group": {"_id": "$metadata.post_id", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"metadata.published": -1, "metadata.post_id": -1}}
    ]

def fetch_documents(collection, pipeline):
    logger.info(f"Fetching documents with mongo pipeline.")
    return list(collection.aggregate(pipeline))

def add_category_to_documents(documents, category_name):
    for doc in documents:
        doc['metadata']['report_category'] = category_name
    return documents

def convert_to_utc_datetime(date_str):
    utc = pytz.UTC
    return utc.localize(datetime.strptime(date_str + " 23:59:59", "%d-%m-%Y %H:%M:%S"))

def fetch_cve_documents_by_product_patterns(collection, start_date, end_date, patterns, exclude_ids=[], use_regex=False):
    if use_regex:
        # print(f"fetch_cves with regex: {patterns}")
        # Compile each pattern into a regex object
        regex_objects = [re.compile(f"\\s*{re.escape(pattern)}\\s*", re.IGNORECASE) for pattern in patterns]
        product_query = {"$regex": "|".join([f"\\s*{re.escape(pattern)}\\s*" for pattern in patterns]), "$options": "i"}
        # print(f"regex based query: {product_query}")
    else:
        product_query = {"$in": patterns} if patterns else None
        regex_objects = [re.compile(re.escape(pattern)) for pattern in patterns]  # Compile for non-regex string match

    query = {
        "metadata.collection": "msrc_security_update",
        "metadata.published": {"$gte": start_date, "$lte": end_date},
        "metadata.id": {"$nin": exclude_ids}
    }
    if product_query:
        query["metadata.products"] = product_query
    
    sort_order = [("metadata.published", -1), ("metadata.post_id", -1)]
    cursor = collection.find(query).sort(sort_order)

    unique_post_ids = set()
    unique_documents = []
    for doc in cursor:
        post_id = doc.get("metadata", {}).get("post_id")
        if post_id not in unique_post_ids:
            unique_documents.append(doc)
            unique_post_ids.add(post_id)
    
    # Filter the products from the products array that match any regex pattern
    for doc in unique_documents:
        mongo_document_products = doc.get("metadata").get("products", [])
        filtered_document_products = []

        for product in mongo_document_products:
            if any(regex.match(product) for regex in regex_objects):
                filtered_document_products.append(product)
                # print(f"product: {product} matches one of the patterns")
            else:
                # print(f"product: {product} does not match the patterns")
                pass

        doc["metadata"]["products"] = filtered_document_products

    return unique_documents


def identify_exclusive_cve_documents(collection, start_date, end_date, include_patterns, exclude_patterns):
    """Identify documents that match include patterns but not exclude patterns."""
    include_docs = fetch_cve_documents_by_product_patterns(collection, start_date, end_date, include_patterns)
    # print("---include---")
    # for doc in include_docs:
    #     print(f"{doc['metadata']['id']}-{doc['metadata']['source']}")
    exclude_docs = fetch_cve_documents_by_product_patterns(collection, start_date, end_date, exclude_patterns)
    # print("---exclude---")
    # for doc in exclude_docs:
    #     print(f"{doc['metadata']['id']}-{doc['metadata']['source']}")
    exclude_ids = [doc['metadata']['id'] for doc in exclude_docs]
    exclusive_docs = [doc for doc in include_docs if doc['metadata']['id'] not in exclude_ids]
    exclusive_ids = [doc['metadata']['id'] for doc in exclusive_docs]
    # print("---keep---")
    # for doc in exclusive_docs:
    #     print(f"{doc['metadata']['id']}-{doc['metadata']['source']}")
    return exclusive_docs, exclusive_ids

def fetch_product_build_documents_by_product_patterns(collection, start_date, end_date, cve_ids, patterns, exclude_ids=[], use_regex=False):
    """
    Fetch documents by product patterns, supporting both simple string and regex matching.
    :param collection: The MongoDB collection.
    :param start_date: Start date for filtering documents. If None, no start date filter is applied.
    :param end_date: End date for filtering documents. If None, no end date filter is applied.
    :param cve_ids: List of CVE IDs to include in the filter.
    :param patterns: List of strings or regex patterns for matching.
    :param exclude_ids: List of document IDs to exclude.
    :param use_regex: Boolean indicating whether to use regex matching.
    :return: List of fetched documents.
    """
    # Date filtering
    date_query = {}
    if start_date is not None:
        date_query["$gte"] = start_date
    if end_date is not None:
        date_query["$lte"] = end_date

    # CVE ID and exclude IDs filtering
    query = {
        "cve_id": {"$in": cve_ids},
        "product_build_id": {"$nin": exclude_ids}
    }

    # Add date query to the main query if any date criteria was provided
    if date_query:
        query["published"] = date_query

    # Product matching
    if use_regex:
        # Convert each pattern into a MongoDB regex query
        adjusted_patterns = [f"\\s*{re.escape(pattern)}\\s*" for pattern in patterns]
        product_query = {"$regex": "|".join(adjusted_patterns), "$options": "i"}
    else:
        # Simple string matching using the '$in' operator
        product_query = {"$in": patterns} if patterns else None

    if product_query:
        query["product"] = product_query

    # Execute query and sort results
    sort_order = [("cve_id", -1)]
    cursor = collection.find(query).sort(sort_order)
    product_build_documents = list(cursor)
    # unique_product_build_ids = set()
    # unique_product_builds = []
    # for doc in cursor:
    #     product_build_id = doc.get("product_build_id")
    #     if product_build_id not in unique_product_build_ids:
    #         unique_product_builds.append(doc)
    #         unique_product_build_ids.add(product_build_id)

    return product_build_documents

def format_build_numbers(build_numbers, sort=True):
    if build_numbers is None:
        return []
    if sort:
        # Convert each list to a tuple (for sorting), sort them, then convert back to list
        build_numbers = sorted([tuple(bn) for bn in build_numbers], reverse=True)
    # Convert each tuple or list back to a string with elements joined by a dot
    return [".".join(map(str, bn)) for bn in build_numbers]


def sort_products(products):
    if products is None:
        return []  # Handle cases where there are no products

    # Helper function to parse and extract sorting keys using improved logic
    def parse_product(product):
        # Simulate the row for compatibility with the split_product_name function
        row = {'product': product}
        # Apply the advanced parsing logic
        parsed_product = split_product_name(row)
        
        # Extract sorting keys
        os_version = parsed_product['product_version']
        architecture = parsed_product['product_architecture']
        product_name = parsed_product['product_name']

        # The sorting tuple prioritizes version, then architecture, and finally the product name for sorting
        return (os_version, architecture, product_name, product)

    # Parse all products, sort them, and extract the original product strings
    sorted_products = sorted(products, key=parse_product)
    return sorted_products

def split_product_name_string(product_string):
    parts = product_string.replace(",", "").split()

    # Initialize default parts
    product_name, product_version, product_architecture = "", "", ""

    try:
        # Attempt to find version index with more flexible conditions
        version_index = next(i for i, part in enumerate(parts) if part[0].isdigit() and (len(part) == 4 or 'H' in part))
        product_name = " ".join(parts[:version_index])
        product_version = parts[version_index]
        product_architecture = " ".join(parts[version_index + 1:])
    except StopIteration:
        # Handle cases without a clear version number
        if 'for' in parts:
            for_index = parts.index('for')
            product_name = " ".join(parts[:for_index])
            product_architecture = " ".join(parts[for_index + 1:])
        else:
            product_name = product_string

    # Additional processing for architecture part
    if product_architecture.startswith("for "):
        product_architecture = product_architecture[4:]

    # Special handling if 'Edition' exists
    if 'Edition' in parts:
        edition_index = parts.index('Edition') + 1
        product_architecture = " ".join(parts[edition_index:])

    # Cleanup and formatting
    product_name = product_name.replace(" version", "").replace(" Version", "").replace(" Edition", "").strip()
    product_name = product_name.lower()
    product_architecture = product_architecture.lower().replace(" ", "_").replace("(", "").replace(")", "").strip()
    product_version = product_version.strip()
    product_str = (product_name + " " + product_version + " " + product_architecture).strip()
    return product_str

def fetch_and_merge_kb_data(post_id, product_build_df):
    # print(f"fetching kb data for {post_id}")
    # Filter the DataFrame for rows where 'cve_id' matches the post_id
    dataframe_slice = product_build_df[product_build_df['cve_id'] == post_id].copy()
    
    # Store (kb_id, article_url) pairs in a list of dictionaries
    kb_article_pairs = []
    for _, row in dataframe_slice.iterrows():
        kb_id = row['kb_id']
        published_date = row['published']  # Assuming 'published' is already a datetime object
        published_str = published_date.strftime('%d-%m-%Y')
        article_url = row.get('article_url')  # Adjust 'article_url' if the column name is different
        
        # Convert published date to the desired format for the URL fragment
        url_fragment = published_date.strftime('#%B-%d-%Y').lower()

        if 'CVE' in kb_id:
            label = f'Release Note ({published_str})'
            # Append the URL fragment to the article_url
            modified_url = f"{article_url}{url_fragment}"
            kb_article_pairs.append({'kb_id': label, 'kb_link': modified_url})
        else:
            # This is a regular kb_id
            modified_url = f"{article_url}{url_fragment}"
            kb_article_pairs.append({'kb_id': kb_id, 'kb_link': modified_url})

    # Sort the list of dictionaries by 'kb_id' in descending order before deduplication
    # Using string comparison for sorting
    kb_article_pairs.sort(key=lambda x: x['kb_id'], reverse=True)
    
    # Deduplicate the list of dictionaries based on 'kb_id' and 'kb_link'
    unique_pairs = {(pair['kb_id'], pair['kb_link']): pair for pair in kb_article_pairs}.values()
    
    return list(unique_pairs)

def fetch_package_pairs(post_id, product_build_df, update_package_df):
    if post_id is None:
        return []
    # print(f"getting update packages for {post_id}")

    dataframe_slice = product_build_df[product_build_df['cve_id'] == post_id].copy()
    product_build_ids = dataframe_slice['product_build_id'].tolist()
    # print(f"found {len(product_build_ids)} product_build_ids\n{product_build_ids}")
    update_package_data_for_cve = update_package_df[update_package_df['product_build_id'].isin(product_build_ids)]

    # A dictionary to hold package information, keyed by package_url
    packages_by_url = {}

    for _, package_row in update_package_data_for_cve.iterrows():
        package_type = package_row['package_type']
        package_url = package_row['package_url']
        kbid_match = re.search(r'KB(\d+)', package_url)
        kbid = int(kbid_match.group(1)) if kbid_match else 0

        # Add to package_type if kbid found
        if kbid_match:
            package_type += f" ({kbid_match.group(0)})"

        # If this URL hasn't been seen before, initialize a new entry
        if package_url not in packages_by_url:
            packages_by_url[package_url] = {
                'package_type': package_type,
                'package_url': package_url,
                'kbid': kbid,
                'downloadable_packages': []
            }

        # Now, add the downloadable packages to this URL's list
        if 'downloadable_packages' in package_row and isinstance(package_row['downloadable_packages'], list):
            for downloadable_package in package_row['downloadable_packages']:
                # Build the downloadable_package dictionary with only the required keys
                downloadable_package_dict = {
                    'product_name': downloadable_package.get('product_name', ''),
                    'product_version': downloadable_package.get('product_version', ''),
                    'product_architecture': downloadable_package.get('product_architecture', ''),
                    'update_type': downloadable_package.get('update_type', ''),
                    'file_size': downloadable_package.get('file_size', ''),
                    'restart_behavior': downloadable_package.get('restart_behavior', ''),
                    'request_user_input': downloadable_package.get('request_user_input', ''),
                    'installed_exclusively': downloadable_package.get('installed_exclusively', ''),
                    'requires_network': downloadable_package.get('requires_network', ''),
                    'uninstall_notes': downloadable_package.get('uninstall_notes', ''),
                }
                existing_packages = packages_by_url[package_url]['downloadable_packages']
                if not any(downloadable_package_dict.items() <= existing.items() for existing in existing_packages):
                    packages_by_url[package_url]['downloadable_packages'].append(downloadable_package_dict)

    # Sort the downloadable packages based on the provided hierarchy
    for package in packages_by_url.values():
        # First, sort by the keys in ascending order that are not `update_type`
        package['downloadable_packages'].sort(
            key=lambda x: (
                x['product_name'],
                x['product_version'],
                x['product_architecture']
            )
        )
        # Then, sort by `update_type` in descending order, which will maintain the order of the other keys due to the stability of Python's sort
        package['downloadable_packages'].sort(
            key=lambda x: x['update_type'], reverse=True
        )

    # Convert the packages_by_url into a list of package pairs and sort by kbid
    package_pairs = list(packages_by_url.values())
    package_pairs.sort(key=lambda x: x['kbid'], reverse=True)
    # print(f"found package pairs for {post_id}: {package_pairs}")
    return package_pairs




def fetch_baseline_value(monitoring_collection, field_name):
    # Fetch the baseline value for a field from the monitoring collection
    monitoring_document = monitoring_collection.find_one(
        {"field_name": field_name},
        {"values": 1}
    )
    # Search within the 'values' array for the subdocument where 'is_baseline' is True
    if monitoring_document:
        for value_info in monitoring_document['values']:
            if value_info.get('is_baseline'):
                return value_info['value']
    return None

def annotate_package_pairs(row, monitoring_collection, field_to_check='restart_behavior'):
    # Fetch the baseline value for the field from the monitoring collection
    baseline_value = fetch_baseline_value(monitoring_collection, field_to_check)

    # Ensure package_pairs exists and is a list
    if 'package_pairs' in row and isinstance(row['package_pairs'], list):
        for package_pair in row['package_pairs']:
            # Check for the existence of 'downloadable_packages' and that it's a list
            if 'downloadable_packages' in package_pair and isinstance(package_pair['downloadable_packages'], list):
                for downloadable_package in package_pair['downloadable_packages']:
                    # Annotate based on restart_behavior compared to the baseline
                    downloadable_package['restart_behavior_matches_baseline'] = (
                        downloadable_package.get(field_to_check, '') == baseline_value
                    )
    # In case there's no 'package_pairs' or it's not a list, do or print nothing special as per your use case
    return row


def record_report_total(report_date, item_total, filepath="report_totals.csv"):
    input_data = (report_date, item_total)
    hash_object = hashlib.sha256(str(input_data).encode())
    hash_value = hash_object.hexdigest()

    # Check if the file exists, and if not, write the header
    try:
        with open(filepath, 'x', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['hash', 'report_date', 'item_total'])
    except FileExistsError:
        pass  # File already exists, no need to add the header

    # Check if the hash already exists in the file
    hash_exists = False
    with open(filepath, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if hash_value in row:
                hash_exists = True
                break  # No need to check further rows

    # Append the data only if the hash does not exist
    if not hash_exists:
        with open(filepath, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([hash_value, report_date, item_total])
            
def filter_dataframe(df, start_date, end_date, columns_to_keep=['item_total', 'running_average']):
    # print("part of generating weekly plot: filter_dataframe()")
    return df[(df.index >= start_date) & (df.index <= end_date)][columns_to_keep]

def plot_data(ax, df, line_colors, trendline_color):
    # print("part of generating weekly plot: plot_data()")
    # print(f"{df.columns}\n{df.head()}")
    columns_to_keep = ['item_total', 'running_average']  # These should be the actual column names in your dataframe
    line_labels = ['Number of Posts', 'Average']  # Labels used for annotation, should correspond to columns_to_keep
    
    for i, col in enumerate(columns_to_keep):
        ax.plot(df.index, df[col], label=line_labels[i], 
                linestyle='-' if i == 0 else '--', 
                color=line_colors[i] if i < len(line_colors) else trendline_color)

    # Ensure the annotations match the actual column names and dataframe structure
    # for i, (label, col) in enumerate(zip(line_labels, columns_to_keep)):
    #     if col in df.columns:
    #         y_val = df[col].iloc[-1]  # Use the actual column name here
    #         ax.annotate(label, xy=(df.index[-1], y_val), xytext=(10, 5-10*i), textcoords='offset points', 
    #                     color=line_colors[i] if i < len(line_colors) else trendline_color, 
    #                     fontsize=9, ha='left', va='center')
    

def annotate_lines(ax, df, line_labels, line_colors, trendline_color):
    columns_to_keep = ['item_total', 'running_average']  # The actual column names from the dataframe
    # print("part of generating weekly plot: annotate_lines()")
    for i, col in enumerate(columns_to_keep):
        y_val = df[col].iloc[-1]
        # Adjust the x-position offset to move the annotation to the left
        x_offset = -8 if i == 0 else -5  # Example offset, adjust as needed
        ax.annotate(line_labels[i],  # Use the descriptive label for annotation
                    xy=(df.index[-1], y_val), 
                    xytext=(x_offset, 5 - 13 * i), 
                    textcoords='offset points',
                    color=line_colors[i] if i < len(line_colors) else trendline_color, 
                    fontsize=9, 
                    ha='right', 
                    va='center')

def format_x_axis(ax, report_start_date, report_end_date):
    # Set the locator for major ticks to every month and for minor ticks to every Tuesday
    # print(f"part of generating weekly plot: format_x_axis() {report_end_date}")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.TU))
    
    # Set the formatter for major ticks to month names and for minor ticks to "Tue-DD"
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%B'))
    def minor_formatter(x, pos):
        return 'Tue-' + mdates.num2date(x).strftime('%d')
    ax.xaxis.set_minor_formatter(FuncFormatter(minor_formatter))
    ax.set_xlim(report_start_date, report_end_date)
    adjust_tick_params(ax)

def adjust_tick_params(ax):
    # This will need to be defined or revised according to your specifications.
    # Example adjustments for tick parameters:
    # print("part of generating weekly plot: adjust_tick_params()")
    ax.tick_params(axis='x', which='major', length=10, labelsize=10, labelcolor='black', pad=31)
    ax.tick_params(axis='x', which='minor', length=5, labelsize=8, labelcolor='gray', pad=5)
    # Set minor tick labels rotation and alignment
    for label in ax.xaxis.get_minorticklabels():
        label.set_rotation(45)
        label.set_ha('right')
    # Set major tick labels rotation and alignment
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center', va='center')

def customize_plot(ax, fig, figsize):
    # print("part of generating weekly plot: customize_plot()")
    fig.subplots_adjust(bottom=0.3)
    remove_spines(ax)
    ax.yaxis.grid(True, which='major', linestyle='-', linewidth='0.25', color='gray')
    ax.xaxis.grid(True, which='major', linestyle=':', linewidth='0.25', color='gray')
    set_label_rotations(ax)
    plt.tight_layout()

def remove_spines(ax):
    # print("part of generating weekly plot: remove_spines()")
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(True)
    ax.spines['bottom'].set_visible(True)

def set_label_rotations(ax):
    # print("part of generating weekly plot: set_label_rotations()")
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center', va='center')

def finalize_plot(output_file_path):
    # print("part of generating weekly plot: finalize_plot()")
    plt.savefig(output_file_path, bbox_inches='tight', dpi=80)
    plt.close()
        

def plot_running_average(data, output_file_path, report_end_date, figsize=(8, 2.25), weeks_back=11, line_colors=None, trendline_color=None):
    # print("starting plot weekly average")
    report_end_date = pd.to_datetime(report_end_date)
    
    if data.empty:
        print("DataFrame is empty. No data to plot.")
        return

    # Set default colors if none provided
    line_colors = line_colors or ['#72929E']
    trendline_color = trendline_color or '#FF4500'

    # Calculate start date and filter the dataframe
    start_date = report_end_date - DateOffset(weeks=weeks_back)
    filtered_df = filter_dataframe(data, start_date, report_end_date)

    # Initialize the plot
    fig, ax = plt.subplots(figsize=figsize)
    ax.clear() 

    # Plot data
    plot_data(ax, filtered_df, line_colors, trendline_color)

    annotate_lines(ax, filtered_df, ['Number of Posts', 'Average'], line_colors, trendline_color)
    
    # Customize the X-axis
    format_x_axis(ax, start_date, report_end_date)

    # Customize the rest of the plot
    customize_plot(ax, fig, figsize)

    # Save and close the plot
    finalize_plot(output_file_path)
    # print("finished plot weekly average")
    

def clean_appendix_title(title):
    # This will remove non-ASCII characters, which might include your problematic character
    cleaned_title = re.sub(r'[^\x00-\x7F]+', ' ', title)
    # If you specifically want to remove or replace the em dash, you can do so like this:
    # Replace em dash with a standard dash (-) or remove it entirely by replacing with ''
    cleaned_title = cleaned_title.replace('—', '-')
    return cleaned_title

def filter_source_documents(source_documents, report_end_date, day_interval):
    end_date = datetime.strptime(report_end_date, "%d-%m-%Y")
    start_date = end_date - timedelta(days=day_interval)
    source_documents['published'] = pd.to_datetime(source_documents['published'], dayfirst=True)
    filtered_docs = source_documents[(source_documents['published'] >= start_date) & (source_documents['published'] <= end_date)]
    return filtered_docs

def prepare_plot_data(source_documents, new_order):
    source_documents['weekday'] = source_documents['published'].dt.day_name()
    weekdays_df = pd.DataFrame(new_order, columns=['weekday'])
    counts_df = source_documents.groupby('weekday').size().reset_index(name='counts')
    final_df = weekdays_df.merge(counts_df, on='weekday', how='left').fillna(0)
    final_df['weekday'] = pd.Categorical(final_df['weekday'], categories=new_order, ordered=True)
    return final_df.sort_values('weekday')

def generate_and_save_plot(final_df, output_file_path, report_end_date, day_interval):
    # Calculate the start date using the day_interval
    report_end_date_dt = datetime.strptime(report_end_date, "%d-%m-%Y")
    start_date = report_end_date_dt - timedelta(days=day_interval - 1)
    
    # Format the date range for the title
    date_range = f"({start_date.strftime('%B %d')} - {report_end_date_dt.strftime('%B %d')})"
    
    custom_palette = ['#72929E', '#82B796', '#8188AC', '#A7BCC4', '#72929E']
    sns.set_theme(style='whitegrid')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=final_df, x='counts', y='weekday', hue='weekday', palette=custom_palette, ax=ax, legend=False)
    ax.xaxis.grid(False)
    # Annotations for each bar with count
    for p in ax.patches:
        ax.annotate(f"{int(p.get_width())}", 
                    (p.get_width(), p.get_y() + p.get_height() / 2), 
                    ha='left', va='center', 
                    size=15, xytext=(5, 0), 
                    textcoords='offset points')
    
    # Remove the hue legend
    # ax.get_legend().remove()

    # Labeling
    ax.set_xlabel('Number of posts per day', fontsize=15)
    ax.set_ylabel('')  # Y-axis should not have a label
    ax.set_title(date_range, fontsize=18)

    # Annotate the total number of posts on the plot
    total_posts = final_df['counts'].sum()
    max_count = final_df['counts'].max()
    if 'Monday' in final_df['weekday'].values:
        # Find the Monday bar's y coordinate
        monday_y = final_df[final_df['weekday'] == 'Monday'].index[0]
        ax.annotate(f'Total: {int(total_posts)}', xy=(max_count, monday_y), xycoords='data', 
                    ha='right', va='center', fontsize=15, bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="b", lw=2),
                    xytext=(-10, -20), textcoords='offset points')
    else:
        # If no Monday bar, place the annotation at the bottom right
        ax.annotate(f'Total: {int(total_posts)}', xy=(0.95, 0), xycoords='axes fraction', 
                    ha='right', va='bottom', fontsize=15, bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="b", lw=2))

    plt.xticks(fontsize=12)
    plt.yticks(fontsize=12)

    # Save the figure
    plt.savefig(output_file_path, bbox_inches='tight', dpi=80)
    plt.close()

def collect_metadata(source_documents):
    total_cve = source_documents.shape[0]
    new_cve_df = source_documents[source_documents['revision'] == 1.0]
    updated_cve_df = source_documents[source_documents['revision'] > 1.0]
    return {
        "total_cves": total_cve,
        "new_cves": new_cve_df.shape[0],
        "updated_cves": updated_cve_df.shape[0],
    }
    
def remove_empty_package_pairs(package_pairs):
    # This will return only those package pairs which have non-empty downloadable_packages
    return [pair for pair in package_pairs if pair.get('downloadable_packages')]


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image
import io

def create_thumbnail(html_file_path, thumbnail_path, width_cm=6, headless=True):
    """
    Takes a screenshot of an HTML report and saves a thumbnail image.

    :param html_file_path: The file path to the local HTML report.
    :param thumbnail_path: The file path to save the thumbnail image.
    :param width_cm: The desired thumbnail width in centimeters.
    """
    margin_cm = 2  # 2cm margin on each side
    dpi = 96  # standard screen resolution
    margin_inch = margin_cm / 2.54  # convert cm to inches
    margin_px = int(margin_inch * dpi)  # convert inches to pixels

    # Set up headless Chrome browser
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")

    # Calculate total window width
    content_width_px = 900
    total_width_px = content_width_px + (2 * margin_px)  # add margins to both sides
    options.add_argument(f"--window-size={total_width_px},1280")

    # Start the browser with WebDriver Manager
    service = Service(ChromeDriverManager().install())
    browser = webdriver.Chrome(service=service, options=options)
    # print(f"create_thumbnail received html path -> {html_file_path}")
    # Open the local HTML file
    browser.get(f"file://{html_file_path}")

    # Take a screenshot of the entire page
    screenshot = browser.get_screenshot_as_png()
    browser.quit()

    # Convert screenshot to an Image object
    screenshot_image = Image.open(io.BytesIO(screenshot))

    # Calculate the thumbnail size, assuming 96 DPI for a typical screen
    dpi = 96
    width_inch = width_cm / 2.54
    width_px = int(width_inch * dpi)
    aspect_ratio = screenshot_image.width / screenshot_image.height
    height_px = int(width_px / aspect_ratio)

    # Resize and save the thumbnail
    thumbnail_image = screenshot_image.resize((width_px, height_px), Image.Resampling.LANCZOS)
    thumbnail_image.save(thumbnail_path)

# Example usage:
# create_thumbnail('/path/to/report.html', '/path/to/thumbnail.png')