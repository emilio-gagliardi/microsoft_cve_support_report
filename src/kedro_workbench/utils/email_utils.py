import re
from datetime import datetime
from dateutil import parser
import fsspec
import json
from icecream import ic


def is_valid_date_format(date_string):
    # Define a regex pattern for "dd-mm-YYYY" format
    date_pattern = r"\d{2}-\d{2}-\d{4}"

    # Use re.match to check if the date string matches the pattern
    if re.match(date_pattern, date_string):
        return True
    else:
        return False


def convert_date_string(date_string):
    if is_valid_date_format(date_string):
        return date_string
    try:
        # First, try parsing using fromisoformat (for ISO 8601 format)
        parsed_date = datetime.fromisoformat(date_string)
        formatted_date = parsed_date.strftime("%d-%m-%Y")
    except ValueError:
        try:
            # If the first attempt fails, try parsing using dateutil's parser.parse
            parsed_date = parser.parse(date_string)
            formatted_date = parsed_date.strftime("%d-%m-%Y")
        except ValueError:
            # Handle parsing errors or invalid input
            formatted_date = date_string
    return formatted_date


def clean_email(body_text):
    # Extract the thread link
    link_pattern = re.compile(
        r"https://groups\.google\.com/d/msgid/patchmanagement/[^\s]+"
    )
    thread_link = None
    for link_search in link_pattern.finditer(body_text):
        thread_link = link_search.group()

    # Define markers and patterns
    previous_conversation_markers = [
        "From: patchmanagement@googlegroups.com",
        "You received this message because you are subscribed",
        "------ Original Message",
    ]
    previous_conversation_patterns = [
        r"On\s+\w+,\s+\w+\s+\d+,\s+\d{4}\s+at\s+\d+:\d+:\d+\s+.*wrote:",
        r"On\s+\w+,\s+\d+\s+\w+\s+\d{4}\s+at\s+\d+:\d+:\d+\s+[aApP][mM]\s+.*wrote:",
    ]

    result = body_text

    # Remove markers and patterns
    for marker in previous_conversation_markers:
        result = result.split(marker, 1)[0].rstrip()

    for pattern in previous_conversation_patterns:
        result = re.sub(pattern, "", result)

    # Remove quoted content
    result = result.split("> ", 1)[0].strip()

    # Insert the link to this message in the Google group
    result += f"\nLink: {thread_link}"

    return result


def move_files_between_containers(
    azure_blob_credentials, source_path, target_path, list_of_files=None
):

    source_fs = fsspec.filesystem("abfs", **azure_blob_credentials)
    target_fs = fsspec.filesystem("abfs", **azure_blob_credentials)

    if list_of_files is None:
        files_to_move = source_fs.ls(source_path)
    else:
        files_to_move = list_of_files  # Use the provided list_of_files directly

    for file_name in files_to_move:
        source_file_path = f"{source_path}/{file_name}"
        target_file_path = f"{target_path}/{file_name}"
        # Move the file from source to target
        try:
            # Move the file from source to target
            source_fs.mv(source_file_path, target_file_path)

        except OSError as e:
            if e.errno == 2:  # Errno 2: No such file or directory
                ic(f"File {source_file_path} does not exist. Skipping...")
            else:
                print(f"An error occurred while moving {file_name}: {e}")


def write_empty_json_to_blob_container(azure_blob_credentials, target_path):
    # Azure Blob Storage URL
    source_fs = fsspec.filesystem("abfs", **azure_blob_credentials)
    # azure_url = "https://bhgaistorage.blob.core.windows.net/cleaned-emails/placeholder.json"
    # Azure storage options with credentials
    azure_options = {
        "account_name": azure_blob_credentials["account_name"],
        "account_key": azure_blob_credentials["account_key"],
    }

    # Create an empty JSON array
    data = []

    # Serialize the data to a JSON string
    json_data = json.dumps(data)
    container_name = "cleaned-emails"
    blob_name = "placeholder.json"
    # Write the JSON string to the Azure Blob Container
    with source_fs.open(
        f"{container_name}/{blob_name}", "wb", **azure_blob_credentials
    ) as f:
        f.write(json_data.encode("utf-8"))


# create day of week
def get_day_of_week(iso_datetime):
    dt_object = datetime.fromisoformat(iso_datetime)
    day_of_week = dt_object.strftime("%A")  # %A gives the full weekday name
    return day_of_week
