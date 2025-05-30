from dotenv import load_dotenv
from langchain.chat_models import init_chat_model

from utils.catalog import get_owner, get_steward, get_business_glossary
from utils.file_parser import get_schema_and_sample_date
from typing import TypedDict
from pandas import DataFrame


class GlossaryInput(TypedDict):
    schema: dict
    sample_data: DataFrame
    owner: str
    steward: str
    domain: str
    glossary: dict


def map_glossary(file_path: str, domain: str) -> GlossaryInput:
    schema, sample = get_schema_and_sample_date(file_path=file_path)
    owner = get_owner(domain)
    steward = get_steward(domain)
    glossary = get_business_glossary(domain)

    return {
        "schema": schema,
        "sample_data": sample,
        "owner": owner,
        "steward": steward,
        "domain": domain,
        "glossary": glossary,
    }


def map_glossary_node(state: dict) -> dict:
    file_path = state["file_path"]
    domain = state["domain"]
    glossary_context = map_glossary(file_path, domain)

    # Construct a message from the context
    prompt = f"""
    You are a data catalog assistant. Help map schema columns to business glossary terms.

    Domain: {glossary_context["domain"]}
    Owner: {glossary_context["owner"]}
    Steward: {glossary_context["steward"]}

    Schema:
    {glossary_context["schema"]}

    Sample Data:
    {glossary_context["sample_data"]}
    
    Business Glossary:
    {glossary_context["glossary"]}

    If columns match existing glossary terms, map them.
    If not, suggest new glossary entries.
    Return structured JSON with:
    - mapped_columns
    - new_glossary_entries
    - new_domain (if applicable)
    """

    load_dotenv()

    llm = init_chat_model("anthropic:claude-3-5-sonnet-latest")
    response = llm.invoke(prompt)
    return {"messages": [{"role": "user", "content": response.content}]}
