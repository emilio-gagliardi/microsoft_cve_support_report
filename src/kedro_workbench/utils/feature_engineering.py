from datetime import datetime
import re
import spacy
import talon
from talon import quotations
from email.parser import Parser
import urllib.parse
from talon.signature.bruteforce import extract_signature
from email_reply_parser import EmailReplyParser
import emoji
from tenacity import retry, wait_exponential, stop_after_attempt
from rake_nltk import Rake
import statistics
import json
import pandas as pd
import ast

nlp = spacy.load("en_core_web_md")

# get day of week from iso timestamp
def get_day_of_week(datetime_str):

    try:
        # Try parsing as ISO timestamp
        dt_object = datetime.fromisoformat(datetime_str)
        day_of_week = dt_object.strftime("%A")  # %A gives the full weekday name
        return day_of_week
    except ValueError:
        try:
            # Try parsing as the second datetime format
            dt_object = datetime.strptime(datetime_str, "%a, %d %b %Y %H:%M:%S %z")
            day_of_week = dt_object.strftime("%A")  # %A gives the full weekday name
            return day_of_week
        except ValueError:
            try:
                # Try parsing as the third datetime format
                dt_object = datetime.strptime(datetime_str, "%B-%d-%Y")
                day_of_week = dt_object.strftime("%A")  # %A gives the full weekday name
                return day_of_week
            except ValueError as e:
                print(f"Date string problem: {e}")
                return "NaT"


def get_sentences(text):
    doc = nlp(text)
    sentences = [sent.text for sent in doc.sents]
    return sentences


def get_unique_tokens(text):
    doc = nlp(text)
    unique_tokens = set(token.lower_ for token in doc if not token.is_punct)
    return " ".join(unique_tokens)


def get_lemmatized_tokens(text):
    doc = nlp(text)
    lemmatized_tokens = [token.lemma_.lower() for token in doc]
    return " ".join(lemmatized_tokens)


def tokenize_text(text):
    # Apply the spaCy tokenizer to the text
    doc = nlp(text)
    # Extract the tokens from the Doc object
    tokens = [token.text for token in doc]
    return tokens


def remove_punctuation(text):
    result = "".join(u for u in text if u not in ("?", ".", ";", "!", '"', ",", "(", ")", "[", "]", "<", ">", "-", "–")
    )
    return result


def deEmojify(x):
    regrex_pattern = re.compile(
        pattern="["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "]+",
        flags=re.UNICODE,
    )
    return regrex_pattern.sub(r"", x)


def remove_emojis(text):
    cleaned_text = emoji.demojize(text)
    return cleaned_text


def unify_whitespaces(text):
    cleaned_string = re.sub(" +", " ", text)
    return cleaned_string


def replace_newline(match):
    if " " in match.group(0):
        return ""
    else:
        return " "


def remove_basic_markup(text):
    """Remove hyperlinks and markup"""
    # result = re.sub("<[a][^>]*>(.+?)</[a]>", 'Link.', raw)
    result = re.sub("&gt;", "", text)
    result = re.sub("&#x27;", "'", result)
    result = re.sub("&quot;", '"', result)
    result = re.sub("&#x2F;", " ", result)
    result = re.sub("<p>", " ", result)
    result = re.sub("</i>", "", result)
    result = re.sub("&#62;", "", result)
    result = re.sub("<i>", " ", result)
    result = re.sub(r"\xa0", " ", result)
    result = re.sub(r"\u202f", " ", result)

    return result


def remove_previous_replies(text):
    previous_conversation_patterns = [
        r"On\s+\w+,\s+\w+\s+\d+,\s+\d{4}\s+at\s+\d+:\d+\s*[aApP][mM]\s+.*wrote:",
        r"On\s+\w+,\s+\d+\s+\w+\s+\d{4}\s+at\s+\d+:\d+:\d+\s+[aApP][mM]\s+.*wrote:",
        r"On\s+\w+,\s+\w+\s+\d+,\s+\d{4}\s+at\s+\d+:\d+\u202f?\s*\w{2}\s+",
        r"On\s+\w+,\s+\d+\s+\w+\s+\d{4}\s+at\s+\d+:\d+,\s+[\w\s]+<[\w@.]",
        r"On\s+\w+,\s+\w+\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s+[APap][Mm]",
        r"From:.*\nSent:.*\nTo:.*\nSubject:.*\n",
        r"From:.*\nSent:.*\nTo:.*\nCc:.*\nSubject:.*\n",
        r"From:.*\nSent:.*\nTo:.*\nSubject:.*\n",
        r"From:.*\nSent:.*\nSubject:.*\n",
        r"From:.*\nSent:.*\nTo:.*\n",
        r"From:.*\nSent:.*\n",
        r"From:.*\nTo:.*\nSubject:.*\n",
        r"From:.*\nTo:.*\nCc:.*\nSubject:.*\n",
        r"From:.*\nTo:.*\nSubject:.*\n",
        r"From:.*\nSubject:.*\n",
        r"From:.*\nTo:.*\n",
        r"From:.*\n",
        r"Sent:.*\nTo:.*\nSubject:.*\n",
        r"Sent:.*\nTo:.*\nCc:.*\nSubject:.*\n",
        r"Sent:.*\nTo:.*\nSubject:.*\n",
        r"Sent:.*\nSubject:.*\n",
        r"Sent:.*\nTo:.*\n",
        r"Sent:.*\n",
        r"To:.*\nSubject:.*\n",
        r"To:.*\nCc:.*\nSubject:.*\n",
        r"To:.*\nSubject:.*\n",
        r"Subject:.*\n",
        r"On\s+\d{1,2}/\d{1,2}/\d{2},\s+\w+\s+\w+\s+wrote:",
        r">\s*(.*)\n",
        r"\s?-{2,}\s?Original Message\s?-{2,}\s?",
        r"(?i)Sent from my iPhone",
    ]
    for pattern in previous_conversation_patterns:
        match = re.search(pattern, text)
        if match:
            text = text[: match.start()]
    return text


def remove_stopwords(text):
    # Apply the spaCy tokenizer to the text
    doc = nlp(text)
    # Remove stop words from the Doc object
    tokens = [token.text for token in doc if not token.is_stop]
    return " ".join(tokens)


def get_noun_phrases(text):
    doc = nlp(text)
    noun_phrases = [
        chunk.text for chunk in doc.noun_chunks if len(chunk.text.split()) > 1
    ]
    return ", ".join(noun_phrases)


def get_direct_objects(text):
    doc = nlp(text)
    direct_objects = [token.text for token in doc if token.dep_ == "DOBJ"]
    return " ".join(direct_objects)


def extract_conversation_link(text):
    pattern1 = r"Link: (https?://groups\.google\.com/d/msgid/patchmanagement/[^ ?]+)"
    match1 = re.search(pattern1, text)
    
    if match1:
        link = match1.group(1)
        # Trim any querystring data
        link = link.split('?')[0]
        return link[len("Link: "):]

    pattern2 = r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    matches = re.findall(pattern2, text)

    if matches:
        for match in matches:
            url = urllib.parse.urlparse(match)
            if url.netloc == "groups.google.com" and url.path.startswith("/d/msgid/patchmanagement/"):
                # Strip the query string
                url_without_query_string = urllib.parse.urlunparse(url._replace(query=""))
                return url_without_query_string

    return ''


def remove_pattern_and_get_text_after(text):
    pattern = r"\[?\s*(https?://aka\.ms/LearnAboutSenderIdentification)\s*\]?"
    # Use re.search to find the pattern in the text
    match = re.search(pattern, text)

    if match:
        # Extract the start position of the match
        start = match.start()
        # Return the text after the match
        cleaned_text = text[start + len(match.group(0)) :]
        return cleaned_text.strip()  # Strip any leading/trailing whitespace
    else:
        return text


def remove_safe_links(text):
    # Regular expression pattern to find URLs enclosed in square brackets
    pattern = r"\[(https?://[^]]*safelinks\.protection\.outlook\.com[^]]*)\]"
    # Replace all matches in the text with an empty string
    return re.sub(pattern, "", text)


def remove_image_links(text):
    # Regular expression pattern for URLs ending with .jpg, .png, or .gif
    pattern = re.compile(r"\[([^]]+\.(?:jpg|jpeg|png|gif))\]")
    # Substitute found patterns with an empty string
    return pattern.sub("", text)


def remove_first_sentence(topic, text):
    pattern = re.escape(topic)

    # Use re.sub() to remove the first occurrence of the subject
    result = re.sub(pattern, "", text, count=1)
    return result


def text_clean_pipeline(df, result_col_name):
    # create an isolated copy of the dataframe
    df_processed = df.copy()
    # pass the to-be-cleaned text into the new column so that you apply the preprocessing on the new column
    df_processed[result_col_name] = df_processed["email_text"]
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        quotations.extract_from_plain
    )
    df_processed["conversation_link"] = df_processed[result_col_name].apply(
        extract_conversation_link
    )
    df_processed[result_col_name] = df_processed["email_text"]
    df_processed[result_col_name] = df_processed.apply(
        lambda row: remove_first_sentence(row["topic"], row[result_col_name]), axis=1
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        EmailReplyParser.parse_reply
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(deEmojify)
    df_processed[result_col_name] = df_processed[result_col_name].apply(remove_emojis)
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        remove_previous_replies
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        remove_pattern_and_get_text_after
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        remove_safe_links
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        remove_image_links
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        remove_basic_markup
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        unify_whitespaces
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        remove_google_group_link
    )
    df_processed[result_col_name] = df_processed[result_col_name].apply(
        remove_phone_numbers_and_emails
    )

    return df_processed


def get_unique_tokens(text):
    doc = nlp(text)
    tokens = [token.lower_ for token in doc if not token.is_punct]
    tokens_no_spaces = [token for token in tokens if token.strip() != ""]
    unique_tokens = set(tokens_no_spaces)
    return list(unique_tokens)


def remove_google_group_link(text):
    # Pattern a) for the link with a URL
    pattern = r"Link:\s(?:https?://[\w./-]+|No Link)"

    # Use re.sub to remove the pattern from the text
    text_without_links = re.sub(pattern, "", text)
    return text_without_links.strip()


def remove_phone_numbers_and_emails(text):
    # Pattern to match phone numbers in various formats
    phone_number_patterns = [
        r"\b(?:tel:)?\d{3}[-. ]?\d{3}[-. ]?\d{4}\b",
        r"\+\d\s*\(\d{3}\)\s*\d{3}-\d{4}",
        r"\(\s*\d{3}\s*\)\s*\d{3}-\d{4}\sx:\d{3}",
    ]

    # Pattern to match email addresses
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"

    # Use re.sub to replace phone numbers and email addresses with an empty string
    for pattern in phone_number_patterns:
        text_without_numbers_and_emails = re.sub(pattern, "", text)
    text_without_numbers_and_emails = re.sub(
        email_pattern, "", text_without_numbers_and_emails
    )

    return text_without_numbers_and_emails.strip()


def extract_keywords(text):
    r = Rake()
    r.extract_keywords_from_text(text)
    return r.get_ranked_phrases_with_scores()

def convert_string_to_object(input_string):
    # print(f"Converting string to object: {input_string}")
    try:
        result = ast.literal_eval(input_string)
        return result
    except (SyntaxError, ValueError):
        print(f"Error: The input string is not a valid representation of a Python object.{input_string}")
        return None
    
def filter_tuples(tuple_list_str):
    tuple_list = convert_string_to_object(tuple_list_str)
    
    if len(tuple_list) > 0:
        if len(tuple_list) > 10:
            scores = [score for score, _ in tuple_list]
            mean = statistics.mean(scores)
            std_dev = statistics.stdev(scores)
            lower_bound = mean - 2 * std_dev
            result = [(score, keyword) for score, keyword in tuple_list if score >= 2 and score >= lower_bound]
            return result
        
        elif len(tuple_list) == 1:
            return tuple_list
        
        else:
            result = [t for t in tuple_list if t[0] >= 2]
            
            return result
    else:
        return []

def extract_json_from_text(text):
    # Find the first '{' character
    start_index = text.find('{')
    if start_index != -1:
        # Find the last '}' character
        end_index = text.rfind('}')
        if end_index != -1:
            # Extract the JSON substring
            json_string = text[start_index:end_index + 1]
            return json_string
    return None

def extract_post_type(row, column_name, key):
    # Parsing the JSON object in the specified column
    json_payload = row[column_name]
    return json_payload.get(key, None)

def extract_summary(row, column_name, key):
    # Parsing the JSON object in the specified column
    # print(row)
    json_payload = row[column_name]
    summary = json_payload.get(key, None)
    
    return summary