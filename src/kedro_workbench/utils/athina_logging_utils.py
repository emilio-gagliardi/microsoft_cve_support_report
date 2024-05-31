"""
This module defines data classes and other utilties to hold configuration details in order to submit logging API calls with the inputs and outputs when using an (LLM) to Athina.

Documentation for Athina logging attributes is here: https://docs.athina.ai/logging/logging-attributes

I have arbitrarily grouped them into 3 buckets to help represent the different types of data that can be logged. RequiredParams, EvalParams, AthinaMeta
In practice, most of the attributes are passed to the AthinaMeta class.

Classes:
    AthinaRequiredParams: Contains essential parameters for logging LLM interactions.
        - language_model_id (str): Identifier for the language model.
        - prompt (List[Dict[str, str]]): List of dictionaries representing the prompt input.
        - response (Dict[str, Any]): Dictionary representing the response from the LLM.

    AthinaEvalParams: Contains optional parameters for evaluating LLM interactions.
        - user_query (Optional[str]): The user's query.
        - context (Optional[Dict[str, Any]]): Additional context for the evaluation.

"""
from dataclasses import dataclass
from typing import List, Dict, Optional, Any


@dataclass
class AthinaParams:
    language_model_id: str
    prompt: List[Dict[str, str]]
    response: Optional[Dict[str, Any]] = None
    user_query: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    cost: Optional[float] = None
    response_time: Optional[float] = None
    prompt_slug: Optional[str] = None
    environment: Optional[str] = None
    customer_id: Optional[str] = None
    customer_user_id: Optional[str] = None
    session_id: Optional[str] = None
    external_reference_id: Optional[str] = None
    custom_attributes: Optional[Dict[str, Any]] = None
    expected_response: Optional[Dict[str, Any]] = None
    tools: Optional[Dict[str, Any]] = None
    tool_calls: Optional[Dict[str, Any]] = None
    functions: Optional[Dict[str, Any]] = None
    function_call_response: Optional[Dict[str, Any]] = None
