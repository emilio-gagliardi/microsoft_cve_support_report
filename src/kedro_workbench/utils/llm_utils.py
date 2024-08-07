import time
import math
from datetime import datetime
from time import sleep
from kedro.config import ConfigLoader
from kedro.framework.project import settings
from kedro_workbench.utils.feature_engineering import (
    convert_string_to_object,
    extract_summary,
)
from kedro_workbench.utils.athina_logging_utils import AthinaParams
import spacy
from sklearn.feature_extraction.text import TfidfVectorizer
import re
from dateutil.parser import parse
from llama_index import Document
from typing import List, Tuple

# import openai
from requests.exceptions import HTTPError

# from openai import OpenAI
import os
import litellm
from litellm import completion as LiteLLMCompletion

# from tenacity import retry, wait_exponential, stop_after_attempt
import tiktoken
import json
import logging
from athina_logger.api_key import AthinaApiKey

# from athina_logger.openai_wrapper import openai
from athina_logger.inference_logger import InferenceLogger
from athina_logger.exception.custom_exception import CustomException

# from athina_logger.athina_meta import AthinaMeta
from dataclasses import dataclass
from pydantic import BaseModel


logger = logging.getLogger(__name__)
encoding = tiktoken.get_encoding("cl100k_base")
nlp = spacy.load("en_core_web_md")

conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
parameters = conf_loader["parameters"]
credentials = conf_loader["credentials"]
openai_api_key = credentials["OPENAI"]["api_key"]
os.environ["OPENAI_API_KEY"] = openai_api_key
athina_credentials = credentials["athina"]
athina_api_key = athina_credentials["api_key"]
AthinaApiKey.set_api_key(athina_api_key)


@dataclass
class LLMParams:
    model: str
    system_prompt: str
    input_prompt: str
    max_tokens: int
    temperature: float


class StringResponse(BaseModel):
    content: str


class StringListResponse(BaseModel):
    content: list[str]


class JsonResponse(BaseModel):
    content: dict


class JsonListResponse(BaseModel):
    content: list[dict]


def get_num_tokens(text):
    num_tokens = len(encoding.encode(text))
    return num_tokens


def find_matching_keys(document: Document) -> List:
    """
    Find all matching keys in the given document.

    Args:
        document (Document): The document to search for matching keys.

    Returns:
        list: A list of matching keys found in the document.
    """
    patterns = ["cve_link", "msrc_link", "content_link"]

    matching_keys = []
    metadata = document.metadata
    for key in metadata:
        for pattern in patterns:
            if key.startswith(pattern):
                matching_keys.append(key)
                break

    return matching_keys


def compute_tfidf(text):
    doc = nlp(text)

    def find_multi_word_strings(string_list):

        composite_words = []
        for string in string_list:
            # Check if the string contains a sequence of capitalized words
            # if re.match(r'^[A-Z][a-z]+(?:[A-Z][a-z]+)+$', string):
            if re.search(r"[A-Z][a-z]*[A-Z][a-z]*", string):
                composite_words.append(string)
        return composite_words

    stopwords = nlp.Defaults.stop_words
    stopwords_list = list(stopwords) if isinstance(stopwords, set) else stopwords

    return_words = []
    all_tokens = list(
        set([token.text for token in doc if token.text not in stopwords_list])
    )
    multi_all = find_multi_word_strings(all_tokens)
    return_words.extend(multi_all)

    """ nouns = [token.text for token in doc if token.pos_ == "NOUN"]
    multi_nouns = find_multi_word_strings(nouns)
    return_words.extend(multi_nouns) """

    """ adjectives = [token.text for token in doc if token.pos_ == "ADJ"]
    multi_adjectives = find_multi_word_strings(adjectives)
    return_words.extend(multi_adjectives) """

    # pos_labels = [
    #     token.pos_
    #     for token in doc
    #     if token.text.lower() not in stopwords_list
    # ]
    vectorizer = TfidfVectorizer(stop_words=stopwords_list)
    tfidf_matrix = vectorizer.fit_transform([text])
    feature_names = (
        vectorizer.get_feature_names()
        if hasattr(vectorizer, "get_feature_names")
        else vectorizer.vocabulary_.keys()
    )
    tfidf_scores = dict(zip(feature_names, tfidf_matrix.toarray()[0]))
    # sorted_nouns = sorted(nouns, key=lambda x: tfidf_scores.get(x, 0), reverse=True)
    # print(f"total TFIDF scores: {len(tfidf_scores)}\n")
    # print(f"total nouns: {len(nouns)}\n")
    return return_words, tfidf_scores


def extract_nouns(text: str) -> List[str]:
    """
    Extracts nouns from the given text using Spacy NLP library.
    Searches for multiword strings that refer to settings and parameters
    discussed in security updates and other Microsoft update announcements.

    Args:
        text (str): The text from which to extract nouns.

    Returns:
        List[str]: A list of extracted nouns.
    """

    doc = nlp(text)

    def find_multi_word_strings(string_list: List[str]) -> List[str]:
        composite_words = []

        for string in string_list:
            # Check if the string contains a sequence of capitalized words
            if re.search(r"[A-Z][a-z]*[A-Z][a-z]*", string):
                composite_words.append(string)

        return composite_words

    stopwords = nlp.Defaults.stop_words
    stopwords_list = list(stopwords) if isinstance(stopwords, set) else stopwords

    return_words = []
    all_tokens = list(
        set(
            [
                token.text
                for token in doc
                if token.text not in stopwords_list and len(token.text) >= 4
            ]
        )
    )
    multi_all_set = set(find_multi_word_strings(all_tokens))
    return_words.extend(list(multi_all_set))

    return return_words


def extract_entities(text: str) -> List[str]:
    """
    Extracts named entities from the given text using spaCy's named entity recognition.

    Args:
        text (str): The text from which entities are to be extracted.
        threshold (float): The threshold for entity recognition. Default is 0.1.

    Returns:
        Tuple[List[str], List[str]]: A tuple containing two lists
        - sorted_nouns and entities.
        sorted_nouns (List[str]): A list of unique nouns extracted from the given text.
        entities (List[str]): A list of unique entity texts extracted
            from the given text.
    """
    entities = []

    doc = nlp(text)
    for ent in doc.ents:
        label = ent.label_
        if label in ["PERSON", "ORG", "PRODUCT"]:
            entities.append(ent.text)
    return list(set(entities))


def find_cve_patterns(text: str) -> Tuple[List[str], List[str]]:
    """
    Finds CVE patterns in the given text.

    Args:
        text: The text in which to search for CVE patterns.

    Returns:
        A tuple containing two lists. The first list contains cleaned
        fix matches, which are fix patterns for CVEs found in the text.
        The second list contains any CVE matches found in the text
        that were not matched by the fix patterns.
    """
    cve_prefix = "CVE-"
    fix_pattern = r"fix for\s*(CVE-\d{4}-\d+)"
    fix_matches = re.findall(fix_pattern, text, re.MULTILINE)
    cleaned_fix_matches = set(
        [match.replace("\n", "").replace("  ", " ") for match in fix_matches]
    )
    any_cve_matches = set(re.findall(cve_prefix + r"\S+", text))
    duplicates = cleaned_fix_matches.intersection(any_cve_matches)
    for duplicate in duplicates:
        any_cve_matches.remove(duplicate)
    return list(cleaned_fix_matches), list(any_cve_matches)


def clean_named_entities(named_entities: List[str]) -> List[str]:
    """
    Clean the list of named entities by removing any entities that
    match the numeric pattern.

    Args:
        named_entities (List[str]): A list of named entities.

    Returns:
        List[str]: A list of cleaned named entities with numeric items removed.
    """
    pattern = r"^(\d{1,4}\.){1,3}\d{1,3}$"
    cleaned_entities = [
        item
        for item in named_entities
        if not (
            re.match(pattern, item)
            and len(re.findall(r"\d+", item)) == len(item.split("."))
        )
    ]
    cleaned_entities = [entity.replace("\n", "") for entity in cleaned_entities]
    return cleaned_entities


def is_url(string):
    return string.startswith("http://") or string.startswith("https://")


def remove_items_with_escape_characters(input_list):
    escape_pattern = r"^\\[a-zA-Z]"
    return [item for item in input_list if not re.match(escape_pattern, item)]


def remove_keywords(keywords):
    keywords_to_remove = [
        "Anonymous Microsoft",
        "Microsoft",
        "Microsoft Edge",
        "Microsoft Microsoft",
        "Microsoft Corporation",
        "Microsoft Learn",
        "FAQ",
        "FAQs",
        "the Microsoft Knowledge Base",
        "UI",
        "URL",
        "URLs",
        "User Interaction",
        "TCP",
        "IP",
        "the Trusted Site Zone",
        "Systems",
        "the CVSS metric",
        "Windows",
        "the Security Update Guide",
        "Chrome Chrome",
        "Chromium",
        "Microsoft Edge Channel Microsoft Edge Version Based",
        "CVE-2020",
        "CVE-2021",
        "CVE-2022",
        "CVE-2023",
        "CVE",
        "CVEs",
        "Chromium Version Date Released",
        "Microsoft Edge Version Date Released",
        "CVSS:3.1",
        "the Control Panel",
        "Chromium Open Source Software",
        "Microsoft-Windows",
        "Microsoft Windows",
        "Windows Updates",
        "See  Microsoft Technical Security Notifications",
        "Recommended Actions",
        "the Windows Driver",
        "the Microsoft Partner Center",
        "Chromium Version",
        "Microsoft Edge Version Date Released",
        "Confidentiality and Integrity",
        "Low Specialized",
        "https://aka.ms/OfficeSecurityReleases",
        "Cross the Divide Microsoft",
        "Server Core",
        "CNA",
        "CVSS",
        "Base",
        "custom_icon",
        "HTTP",
        "HTTPS",
        "\\nOn this page",
        "On this page",
        "Release Notes",
        "Max Severity",
        "Microsoft Support Lifecycle",
        "Microsoft Edge Version Date",
        "the Confidentiality",
        "Integrity and Authentication",
        "Chrome",
        "Google",
    ]

    # Filter the word_list to remove keywords
    cleaned_list = [word for word in keywords if word not in keywords_to_remove]
    urls_removed_list = [string for string in cleaned_list if not is_url(string)]
    special_chars_removed = remove_items_with_escape_characters(urls_removed_list)
    sorted_list = sorted(special_chars_removed, key=len, reverse=True)

    return sorted_list


def convert_date_string(date_string: str, date_format: str = "%d-%m-%Y") -> str:
    """
    Converts a date string to a specified format.

    Args:
        date_string: The date string to be converted.
        date_format: The format to which the date string will be converted.
        Defaults to "%d-%m-%Y".

    Returns:
        The formatted date string if it can be parsed otherwise it returns "NaT".

    Raises:
        Exception: If an error occurs during the conversion process.
    """
    pattern = r"^\d{2}-\d{2}-\d{4}$"
    formatted_date = None
    if isinstance(date_string, (str, bytes)):
        if re.match(pattern, date_string):
            return date_string
    else:
        try:
            date_object = parse(date_string)
            formatted_date = date_object.strftime(date_format)
        except Exception as e:
            raise ValueError(f"Failed to process date string {date_string}: {str(e)}")

    return formatted_date


def prepare_for_sentence_parser_split(text_to_parse, chunk_size=1024, pattern=". \n"):
    words = text_to_parse.split()
    result = []
    current_chunk_size = 0

    for word in words:
        if current_chunk_size + len(word) + 1 > chunk_size:
            result.append(pattern)
            current_chunk_size = 0
        elif current_chunk_size > 0:
            result.append(" ")
            current_chunk_size += 1

        result.append(word)
        current_chunk_size += len(word)

    return "".join(result)


def prepare_for_sentence_parser_token(
    text_to_parse, nlp, chunk_size=1024, pattern=". \n"
):
    doc = nlp(text_to_parse)
    words = [token.text for token in doc if not token.is_space]
    result = []
    current_chunk_size = 0

    for word in words:
        if current_chunk_size + len(word) + 1 > chunk_size:
            result.append(pattern)
            current_chunk_size = 0
        elif current_chunk_size > 0:
            result.append(" ")
            current_chunk_size += 1

        result.append(word)
        current_chunk_size += len(word)

    return "".join(result)


def prepare_for_sentence_parser_llama_sentence(
    text_to_parse, text_splitter, chunk_size=512
):
    chunks = text_splitter.split_text(text_to_parse)
    result = ""
    for chunk in chunks:
        modified_item = chunk + " \n.\n "
        result += modified_item
    final_string = "".join(result)
    return final_string


# We start with a list of Document objects which are arbitrarily ordered
# and difficult to process for each collection
# build a dictionary based on collection name and create separate lists
# for each collection
# allows for simple access to grouped documents using "collection_names" list
def restructure_documents_by_collection(documents):
    collection_dict = {}
    for doc in documents:
        collection = doc.metadata["collection"]
        if collection not in collection_dict:
            collection_dict[collection] = []
        collection_dict[collection].append(doc)
    return collection_dict


def yield_n_documents(dictionary, n):
    collection_names = sorted(dictionary.keys(), key=lambda key: len(dictionary[key]))
    total_documents = sum(len(dictionary[key]) for key in collection_names)
    documents_per_list = n

    # Calculate the number of iterations needed to yield all documents
    iterations = total_documents // n + (total_documents % n > 0)
    # Create a generator that yields a subset dictionary on each iteration
    for i in range(iterations):
        print(f"iteration {i}")
        result_dict = {key: [] for key in collection_names}
        remaining_documents = documents_per_list

        # Yield 'n' documents from each non-empty list in order
        for key in collection_names:
            doc_list = dictionary[key]
            if doc_list:
                num_documents = min(len(doc_list), remaining_documents)
                result_dict[key] = doc_list[:num_documents]
                doc_list[:num_documents] = []
                remaining_documents -= num_documents
            # Stop yielding documents if we have reached the maximum
            # number of documents for this iteration
            if remaining_documents == 0:
                break

        # Yield the result dictionary for this iteration
        yield result_dict


def fit_prompt_to_window(text, max_tokens):
    tokens = encoding.encode(text)
    num_tokens = len(tokens)

    if num_tokens > max_tokens:
        # Keep only the first max_tokens
        limited_tokens = tokens[:max_tokens]

        # Decode these tokens back into text
        limited_text = encoding.decode(limited_tokens)

        # Use the length of the limited_text to truncate the original text
        truncated_text = text[: len(limited_text)]
        logger.debug(
            f"Prompt was truncated to fit within the max_tokens limit of {max_tokens}"
        )
        return truncated_text
    else:
        # If the original number of tokens is within the limit, return the original text
        return text


def call_llm_completion(model, system_prompt, input_prompt, max_tokens, temperature):

    client = OpenAI(
        api_key=openai_api_key,
    )
    response_msg = ""
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": input_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }

    # print(payload)
    try:
        response = client.chat.completions.create(**payload)
        choice = response.choices[0]
        response_msg = choice.message.content
    except HTTPError as e:
        # If an HTTPError is caught, print out the error and the data sent
        logger.error(f"HTTPError occurred: {e}")
        logger.info(
            f"Max tokens: {max_tokens}\n"
            f"Data sent: system_prompt: {system_prompt}\n"
            f"input_prompt: {input_prompt}\n"
        )
    except Exception as e:
        # Catch other exceptions
        logger.error(f"An exception occurred: {e}")

    sleep(2.5)
    return response_msg


def call_llm_completion_with_logging(
    llm_params: LLMParams, athina_params: AthinaParams
):

    # client = OpenAI(api_key=openai_api_key)
    response_msg = ""
    payload = {
        "model": llm_params.model,
        "messages": [
            {"role": "system", "content": llm_params.system_prompt},
            {"role": "user", "content": llm_params.input_prompt},
        ],
        "max_tokens": llm_params.max_tokens,
        "temperature": llm_params.temperature,
    }

    try:
        start_time = time.time()
        # response = client.chat.completions.create(**payload) # for use with openai direct requests
        response = LiteLLMCompletion(**payload)  # Implement the LiteLLM abstraction
        # TODO: implement Instructor package to enforce output classes
        # significant change to function logic required
        end_time = time.time()
        response_time_ms = (end_time - start_time) * 1000
        choice = response.choices[0]
        response_msg = choice.message.content
        response_dict = response.model_dump()

        log_params = athina_params.__dict__
        log_params.update(
            {
                "prompt": payload["messages"],
                "user_query": llm_params.input_prompt,
                "response": response_dict,
                "language_model_id": llm_params.model,
                "response_time": response_time_ms,
            }
        )

        try:
            InferenceLogger.log_inference(**log_params)

        except CustomException as ce:
            logger.exception(
                f"An Athina CustomException was caught: Status Code: {ce.status_code}, Message: {ce.message}"
            )
        except Exception as e:
            logger.exception(
                f"An unexpected exception occurred while connecting to Athina Inference logging: {str(e)}"
            )

    except HTTPError as e:
        print(f"HTTPError occurred: {e}")
        print(
            f"Max tokens: {llm_params.max_tokens}\n"
            f"Data sent: system_prompt: {llm_params.system_prompt},\n"
            f"input_prompt: {llm_params.input_prompt}"
        )
    except Exception as e:
        print(f"An exception occurred: {e}")
    return response_msg


# testing. adding instructor wrapper around LiteLLM wrapper.
# Instructor enforces output formatting using pydantic data models
# we're testing 3 pydantic data models, one for each type of response
# (1) a list of strings, (2) a json dict,
# and (3) a basic string
# TODO: incorporate Instructor package to enforce LLM output parsing


def get_sample_llm_response():
    file_path = r"src\kedro_workbench\utils\sample_kg_entities.json"
    with open(file_path, "r") as file:
        dicts = json.load(file)
    if "counter" not in get_sample_llm_response.__dict__:
        get_sample_llm_response.counter = 0

    # Get the current dictionary and increment the counter
    current_dict = dicts[get_sample_llm_response.counter % len(dicts)]
    get_sample_llm_response.counter += 1

    # Convert the dictionary to a JSON string and return it
    return json.dumps(current_dict)


def sanitize_output(output, expected_format):
    # Define patterns based on the expected format
    # Something is adding comments around LLM response.
    # This is a workaround to attempt to trim that if it is encountered.

    patterns = {
        "list": (r"^```python\n", r"\n```$"),
        "json": (r"^```json\n", r"\n```$"),
    }
    start_pattern, end_pattern = patterns.get(expected_format, ("", ""))

    # Use a single regex substitution to remove the markers
    sanitized_output = re.sub(f"{start_pattern}|{end_pattern}", "", output)
    # print(f"Sanitzing llm output for {expected_format}\n{sanitized_output}")
    return sanitized_output


def evaluate_rake_keywords(model, row, system_prompt, max_tokens, temperature):
    # Extract text and keywords from the DataFrame row
    text = row["normalized_tokens"]
    keywords = row["filtered_keywords"]
    keywords_list = convert_string_to_object(keywords)
    expected_value = None
    if "expected_value" in row:
        expected_value = row["expected_value"]

    try:
        tokens = str(text).split()
    except Exception as e:
        # Handle the exception here, e.g., print an error message
        tokens = text
        logger.info(
            f"Warning. Could not split 'normalized_tokens' into list of words.{e}"
        )

    if len(tokens) > 2000:
        text = " ".join(tokens[:2000])
        first_20_keywords = keywords_list[:20]
        keywords_string = ", ".join(
            f"({score}, '{text}')" for score, text in first_20_keywords
        )
        input_prompt = f"Given the following keyword tuples: \n---\n{keywords_string}\n---\nThe email text: \n---\n{text}\n---\n, choose the top 7 keywords that communicate the core topic and purpose of the email. Remove keywords that reference the author (eg. 'karl wester'). Remove keywords with credentials (eg., 'philip elder mcts senior technical architect microsoft high availability mvp'). Remove keywords that reference communication channels. Remove semantically empty keywords, remove partial url keywords (eg., 'https :// support' or 'https :// www') and you must remove keywords with dates in them. Ignore all keywords extracted from an email signature. Important. Format your answer as a python list and only respond with the list: [keyword_1, keyword_2, ...]. Do not include any other dialog or language-specific markers in your answer. If there are no good keywords, just output 'None'. Before you finish, evaluate your answer and remove keywords that are substrings of other keywords ['exe exit code', 'exit code'] -> ['exe exit code']."
    elif len(keywords) == 0:
        keywords_string = "None"
        input_prompt = f"There are no keywords for this document to evaluate. Your answer is {keywords_string}"
    else:
        # print(f"evaluating keywords -> {keywords}")
        keywords_string = keywords
        input_prompt = f"Given the following keyword tuples: \n---\n{keywords_string}\n---\nThe email text: \n---\n{text}\n---\nchoose the top 4 keywords that communicate the core topic and purpose of the email.Remove keywords that reference the author (eg. 'karl wester'). Remove keywords with credentials (eg., 'philip elder mcts senior technical architect microsoft high availability mvp'). Remove keywords that reference communication channels. Remove semantically empty keywords, remove partial url keywords (eg., 'https :// support' or 'https :// www') and you must remove keywords with dates in them. Ignore all keywords extracted from an email signature. Important. Format your answer as a python list and only respond with the list: [keyword_1, keyword_2, ...]. Do not include any other dialog or language-specific markers in your answer. If there are no good keywords, just output 'None'. Before you finish, evaluate your answer and remove keywords that are substrings of other keywords ['exe exit code', 'exit code'] -> ['exe exit code']."

    # Define your input prompt to the GPT chat client
    json_safe_input_prompt = json.dumps(input_prompt)
    context = (
        f"Rake keyword tuples (0<=n<=20):\n---\n{keywords_string}\n---\n"
        f"The email text:\n---\n{row['normalized_tokens']}\n"
    )

    row["keyword_context"] = context
    # print(f"keyword context\n{row['keyword_context']}")

    current_datetime = datetime.now()
    llm_params = LLMParams(
        model=model,
        system_prompt=system_prompt,
        input_prompt=json_safe_input_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    custom_logging_attributes = {
        "pipeline": "docstore_feature_engineering_pm",
        "node": "evaluate_rake_keywords",
        "description": "Evaluate and rank a set of Rake generated keywords extracted from a Patch Management email.",
    }
    athina_params = AthinaParams(
        language_model_id=model,
        prompt=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": input_prompt},
        ],
        user_query=None,
        expected_response=expected_value,
        context=row["keyword_context"],
        prompt_slug="evaluate_rake_keywords",
        environment="uplyft_dsm_1",
        customer_id="PortalFuse",
        customer_user_id="emilio.gagliardi",
        session_id=f"report_etl_{current_datetime.strftime('%d_%m_%Y')}",
        custom_attributes=custom_logging_attributes,
    )
    # Generate a response using the GPT chat client
    response_content = call_llm_completion_with_logging(
        llm_params=llm_params, athina_params=athina_params
    )

    sanitized_response_content = sanitize_output(response_content, "list")
    # print(f"response_content: {sanitized_response_content}")
    time.sleep(2)
    return sanitized_response_content


def evaluate_noun_chunks(model, row, system_prompt, max_tokens, temperature):
    # Extract text and keywords from the DataFrame row
    text = row["email_text_clean"]
    noun_chunks_str = row["noun_chunks"]
    expected_value = None
    if "expected_value" in row:
        expected_value = row["expected_value"]
    # context = description + df['noun_chunks'] + description + df['email_text_clean']

    if not isinstance(noun_chunks_str, str):
        noun_chunks_str = ""
        noun_chunks = []
    else:
        noun_chunks = noun_chunks_str.split(", ")
    # print(f"noun_chunks_str: {type(noun_chunks_str)}\n{noun_chunks_str}\n")

    try:
        tokens = str(text).split()
    except Exception as e:
        # Handle the exception here, e.g., print an error message
        logger.info(f"Unable to split noun chunks string: {text} {e}")
        tokens = text

    if len(tokens) > 2000:
        text = " ".join(tokens[:2000])
        first_20_noun_chunks = noun_chunks[:20]
        noun_chunks_string = ", ".join(first_20_noun_chunks)
        input_prompt = f"""Given the following noun chunks: \n---\n{noun_chunks_string}\n---\n, and the email text: \n---\n {text} \n---\n, choose the top 7 noun chunks that communicate the core topic and purpose of the email. Remove noun chunks that reference the author (eg. 'karl wester'). Remove noun chunks with credentials (eg., 'philip elder mcts senior technical architect microsoft high availability mvp'). Remove noun chunks that reference communication channels. Remove semantically empty noun chunks, remove partial url noun chunks (eg., 'https :// support' or 'https :// www') and you must remove noun chunks with dates in them. Ignore all noun chunks extracted from an email signature. Important. Format your answer as a python list and only respond with the list: [keyword_1, keyword_2, ...]. Do not include any other dialog or language-specific markers in your answer. If there are no good noun chunks, just output 'None'. Before you finish, evaluate your answer and remove noun chunks that are substrings of other noun chunks ['exe exit code', 'exit code'] -> ['exe exit code']."""
    elif len(noun_chunks) == 0:
        noun_chunks_string = "None"
        input_prompt = f"""There are no noun chunks for this document to evaluate. Your answer is {noun_chunks_string}"""
    else:
        noun_chunks_string = noun_chunks_str
        input_prompt = f"""Given the following noun chunks: \n---\n{noun_chunks_string}\n---\n, and the email text: \n---\n{text}\n---\n, choose the top 4 noun chunks that communicate the core topic and purpose of the email. Remove noun chunks that reference the author (eg. 'karl wester'). Remove noun chunks with credentials (eg., 'philip elder mcts senior technical architect microsoft high availability mvp'). Remove noun chunks that reference communication channels. Remove semantically empty noun chunks, remove partial url noun chunks (eg., 'https :// support' or 'https :// www') and you must remove noun chunks with dates in them. Ignore all noun chunks extracted from an email signature. Important. Format your answer as a python list and only respond with the list: [keyword_1, keyword_2, ...]. Do not include any other dialog or language-specific markers in your answer. If there are no good noun chunks, just output 'None'. Before you finish, evaluate your answer and remove noun chunks that are substrings of other noun chunks ['exe exit code', 'exit code'] -> ['exe exit code']."""
    context = (
        f"Extracted Noun chunks:\n---\n{noun_chunks_string}\n---\n"
        f"\n---\nThe email text:\n---\n{text}\n"
    )
    row["noun_context"] = context
    # print(f"noun context\n{row['noun_context']}")
    json_safe_input_prompt = json.dumps(input_prompt)
    current_datetime = datetime.now()
    llm_params = LLMParams(
        model=model,
        system_prompt=system_prompt,
        input_prompt=json_safe_input_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    custom_logging_attributes = {
        "pipeline": "docstore_feature_engineering_pm",
        "node": "evaluate_noun_chunks",
        "description": "Evaluate and rank a set of Spacy generated noun chunks extracted from a Patch Management email.",
    }
    athina_params = AthinaParams(
        language_model_id=model,
        prompt=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": input_prompt},
        ],
        user_query=None,
        expected_response=expected_value,
        context=context,
        prompt_slug="evaluate_noun_chunks",
        environment="uplyft_dsm_1",
        customer_id="PortalFuse",
        customer_user_id="emilio.gagliardi",
        session_id=f"report_etl_{current_datetime.strftime('%d_%m_%Y')}",
        custom_attributes=custom_logging_attributes,
    )
    response_content = call_llm_completion_with_logging(
        llm_params=llm_params, athina_params=athina_params
    )
    sanitized_response_content = sanitize_output(response_content, "list")
    # print(f"response_content: {sanitized_response_content}")
    # Store the response back in the DataFrame
    time.sleep(2)
    return sanitized_response_content


def classify_email(
    model,
    system_prompt,
    user_prompt,
    max_tokens,
    temperature,
    context=None,
    expected_value=None,
):
    # Extract text and keywords from the DataFrame row
    # text = row["email_text_clean"]
    # tokens = text.split()
    # classifications_str = ", ".join(classifications)
    # if len(tokens) > 2000:
    #     text = ' '.join(tokens[:2000])

    current_datetime = datetime.now()
    llm_params = LLMParams(
        model=model,
        system_prompt=system_prompt,
        input_prompt=user_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    custom_logging_attributes = {
        "pipeline": "docstore_feature_engineering_pm",
        "node": "classify_email",
        "description": "Classify the content of a Patch Management email as one of 'Helpful tool', 'conversational', 'Problem statement', or 'Solution provided' based on the text and metadata.",
    }
    athina_params = AthinaParams(
        language_model_id=model,
        prompt=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        user_query=None,
        context=context,
        expected_response=expected_value,
        prompt_slug="classify_patch_email",
        environment="uplyft_dsm_1",
        customer_id="PortalFuse",
        customer_user_id="emilio.gagliardi",
        session_id=f"report_etl_{current_datetime.strftime('%d_%m_%Y')}",
        custom_attributes=custom_logging_attributes,
    )
    response_content = call_llm_completion_with_logging(
        llm_params=llm_params, athina_params=athina_params
    )
    # json_safe_input_prompt = json.dumps(input_prompt)
    # llm_response = call_llm_completion(
    #     model,
    #     system_prompt,
    #     user_prompt,
    #     max_tokens,
    #     temperature
    #     )
    # Store the response back in the DataFrame
    json_result = {}

    extracted_json = extract_json_from_text(response_content)
    if extracted_json:
        json_result = json.loads(extracted_json)
        return json_result
    else:
        logger.debug(json_result)

    return json_result


def extract_json_from_text(text):
    # Find the first '{' character
    start_index = text.find("{")
    if start_index != -1:
        # Find the last '}' character
        end_index = text.rfind("}")
        if end_index != -1:
            # Extract the JSON substring
            json_string = text[start_index : end_index + 1]
            return json_string
    return None


def clean_json_string(json_string):
    """
    Remove trailing commas from JSON strings to prevent JSONDecodeError.
    """
    # Remove trailing commas in objects
    json_string = re.sub(r",\s*}", "}", json_string)
    # Remove trailing commas in arrays
    json_string = re.sub(r",\s*\]", "]", json_string)
    return json_string


def classify_post(
    model,
    system_prompt,
    user_prompt,
    max_tokens,
    temperature,
    context=None,
    expected_value=None,
):
    current_datetime = datetime.now()
    llm_params = LLMParams(
        model=model,
        system_prompt=system_prompt,
        input_prompt=user_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    custom_logging_attributes = {
        "pipeline": "docstore_classification_msrc",
        "node": "classify_post_msrc_node",
        "description": "Classify a CVE post based on context provided in the input_prompt. 3 classes are available: 'Critical', 'Information only', 'Solution provided'.",
    }
    athina_params = AthinaParams(
        language_model_id=model,
        prompt=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        user_query=None,
        context=context,
        expected_response=expected_value,
        prompt_slug="classify_cve",
        environment="uplyft_dsm_1",
        customer_id="PortalFuse",
        customer_user_id="emilio.gagliardi",
        session_id=f"report_etl_{current_datetime.strftime('%d_%m_%Y')}",
        custom_attributes=custom_logging_attributes,
    )
    # llm_response = call_llm_completion(
    #     model,
    #     system_prompt,
    #     user_prompt,
    #     max_tokens,
    #     temperature
    #     )
    llm_response = call_llm_completion_with_logging(
        llm_params=llm_params, athina_params=athina_params
    )
    time.sleep(2.5)
    # print(f"llm_response\n{llm_response}")
    json_result = {}

    extracted_json = extract_json_from_text(llm_response)
    if extracted_json:
        json_result = json.loads(extracted_json)
        return json_result
    else:
        logger.debug(json_result)
    return json_result


def summarize_post(
    model,
    system_prompt,
    user_prompt,
    max_tokens,
    temperature,
    context=None,
    expected_value=None,
):

    current_datetime = datetime.now()
    llm_params = LLMParams(
        model=model,
        system_prompt=system_prompt,
        input_prompt=user_prompt,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    custom_logging_attributes = {
        "pipeline": "periodic_report_CVE_WEEKLY_v1",
        "node": "summarize_section_1_periodic_report_CVE_WEEKLY_v1",
        "description": "Summarize the CVE post based on the available text and criteria related to their technical nature and severity.",
    }
    athina_params = AthinaParams(
        language_model_id=model,
        prompt=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        user_query=None,
        context=context,
        expected_response=expected_value,
        prompt_slug="summarize_cve",
        environment="uplyft_dsm_1",
        customer_id="PortalFuse",
        customer_user_id="emilio.gagliardi",
        session_id=f"report_etl_{current_datetime.strftime('%d_%m_%Y')}",
        custom_attributes=custom_logging_attributes,
    )
    try:
        llm_response = call_llm_completion_with_logging(
            llm_params=llm_params, athina_params=athina_params
        )
        extracted_json = extract_json_from_text(llm_response)
        if extracted_json:
            try:
                json_result = json.loads(extracted_json)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decoding error: {e}")
                try:
                    corrected_json = extracted_json.encode("utf-8").decode(
                        "unicode_escape"
                    )
                    json_result = json.loads(corrected_json)
                except Exception as e:
                    logger.error(f"Error correcting JSON string: {e}\n{extracted_json}")
                    try:
                        cleaned_json_string = clean_json_string(extracted_json)
                        json_result = json.loads(cleaned_json_string)
                    except Exception as e:
                        logger.error(
                            f"Final attempt failed; proceeding without this record. Error: {e}"
                        )
                        return {}
            except Exception as e:
                logger.error(f"Unexpected error parsing JSON: {e}")
                return {}
        else:
            logger.debug("No JSON extracted from LLM response.")
            return {}
    except Exception as e:
        logger.error(f"Error calling LLM or processing response: {e}")
        return {}

    return json_result


def apply_summarization(
    row,
    model,
    system_prompt,
    max_tokens,
    temperature,
    source_column_name,
    key_to_extract,
):
    # Call summarize_post with the appropriate arguments
    llm_response = summarize_post(
        model,
        system_prompt,
        row["user_prompt"],
        max_tokens,
        temperature,
        row["summarization_context"],
    )

    # Directly assign the llm_response (summarization payload) to the DataFrame
    row[source_column_name] = llm_response

    # Extract the summary from the llm_response using feat_eng.extract_summary
    extracted_summary = extract_summary(row, source_column_name, key_to_extract)
    if (
        extracted_summary is not None
        and extracted_summary != ""
        and extracted_summary != {}
    ):
        # Update the 'summary' column with the extracted summary
        row[key_to_extract] = extracted_summary
    else:
        row[key_to_extract] = None

    return row


def create_metadata_string_for_user_prompt(row, metadata_keys):
    # Extracting values from the row, assuming 'metadata' is a dictionary in the row
    metadata_str = ""
    # print("create metadata string, columns available:")
    # for column_name in row.index:
    #     print(column_name)

    # Check and build string for each key
    if "post_id" in metadata_keys and "post_id" in row and row["post_id"]:
        metadata_str += f"post_id: {row['post_id']}\n"

    if "id" in metadata_keys and "id" in row:
        metadata_str += f"id: {row['id']}\n"

    if "doc_id" in metadata_keys and "doc_id" in row:
        metadata_str += f"id: {row['doc_id']}\n"

    if "revision" in metadata_keys and "revision" in row and row["revision"]:
        metadata_str += f"revision: {row['revision']}\n"

    if "published" in metadata_keys and "published" in row:
        metadata_str += f"published: {row['published']}\n"

    if "topic" in metadata_keys and "topic" in row and row["topic"]:
        metadata_str += f"subject: {row['topic']}\n"

    if "description" in metadata_keys and "description" in row and row["description"]:
        metadata_str += f"description: {row['description']}\n"

    if (
        "conversation_link" in metadata_keys
        and "conversation_link" in row
        and row["conversation_link"]
    ):
        metadata_str += f"conversation_link: {row['conversation_link']}\n"

    if (
        "unique_tokens" in metadata_keys
        and "unique_tokens" in row
        and row["unique_tokens"]
    ):
        metadata_str += f"number unique tokens: {row['unique_tokens']}\n"

    # if 'evaluated_keywords' in metadata_keys and 'evaluated_keywords' in row and row['evaluated_keywords']:
    #     metadata_str += f"rake keywords: {row['evaluated_keywords']}\n"

    if "evaluated_keywords" in metadata_keys and "evaluated_keywords" in row:
        if row["evaluated_keywords"] and not (
            isinstance(row["evaluated_keywords"], float)
            and math.isnan(row["evaluated_keywords"])
        ):
            metadata_str += f"rake keywords: {row['evaluated_keywords']}\n"
        else:
            metadata_str += "rake keywords: None\n"

    if "evaluated_noun_chunks" in metadata_keys and "evaluated_noun_chunks" in row:
        if row["evaluated_noun_chunks"] and not (
            isinstance(row["evaluated_noun_chunks"], float)
            and math.isnan(row["evaluated_noun_chunks"])
        ):
            metadata_str += f"noun chunks: {row['evaluated_noun_chunks']}\n"
        else:
            metadata_str += "noun chunks: None\n"

    if "source" in metadata_keys and "source" in row and row["source"]:
        metadata_str += f"source: {row['source']}\n"

    if (
        "receivedDateTime" in metadata_keys
        and "receivedDateTime" in row
        and row["receivedDateTime"]
    ):
        metadata_str += f"receivedDateTime: {row['receivedDateTime']}\n"

    if "post_type" in metadata_keys and "post_type" in row and row["post_type"]:
        metadata_str += f"post_type: {row['post_type']}\n"

    return metadata_str


def get_prompt(dictionary, key, default_key="default"):
    """
    Get the value for the given key from the dictionary.
    If the key does not exist, return the value for the default key.

    Parameters:
    dictionary (dict): The dictionary to search.
    key (str): The key to look up.
    default_key (str): The default key to use if the key is not found.

    Returns:
    str: The value for the key or the default key.
    """
    return dictionary.get(key, dictionary.get(default_key))
