from dotenv import load_dotenv
from langchain.chat_models import init_chat_model

from utils.catalog import get_owner, get_steward, get_business_glossary
from utils.source_parser import parse_csv, parse_webpage, parse_swagger
from utils.semantic_tagger import infer_semantic_tags
from typing import TypedDict, Literal
from pandas import DataFrame


class GlossaryInput(TypedDict):
    schema: dict
    sample_data: DataFrame
    owner: str
    steward: str
    domain: str
    glossary: dict
    semantic_tags: dict

def get_schema_and_sample_data(source_type: Literal["csv", "webpage", "swagger"], path: str):
    if source_type == "csv":
        return parse_csv(path)
    elif source_type == "webpage":
        return parse_webpage(path)
    elif source_type == "swagger":
        return parse_swagger(path)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def map_glossary(file_path: str, domain: str, source_type: str) -> GlossaryInput:
    schema, sample = get_schema_and_sample_data(source_type, file_path)
    owner = get_owner(domain)
    steward = get_steward(domain)
    glossary = get_business_glossary(domain)
    tags = infer_semantic_tags(schema, sample)

    return {
        "schema": schema,
        "sample_data": sample,
        "owner": owner,
        "steward": steward,
        "domain": domain,
        "glossary": glossary,
        "semantic_tags": tags
    }


def map_glossary_node(state: dict) -> dict:
    file_path = state["file_path"]
    domain = state["domain"]
    source_type = state["source_type"]

    glossary_context = map_glossary(file_path, domain, source_type)

    prompt = f"""
    You are a highly skilled data catalog assistant.

    Context:
    The data you are analyzing is likely from a **social media company**. The source is a {source_type} file.
    Your task is to map all raw schema fields to business glossary terms based on their names,values, description, types, and any semantic hints available.

    Inputs:
    - Domain: {glossary_context["domain"]}
    - Owner: {glossary_context["owner"]}
    - Steward: {glossary_context["steward"]}

    Schema:
    {glossary_context["schema"]}

    Sample Data:
    {glossary_context["sample_data"]}

    Glossary:
    {glossary_context["glossary"]}

    Semantic Tags:
    {glossary_context["semantic_tags"]}

    Instructions:
    - Match fields from the schema/fields to glossary terms if names or types match directly or closely.
    - If no match is found, suggest a new glossary entry with appropriate business term, definition (inferred from field name and context), data type, and example.
    - Output should include semantic tags for each field.

    If columns match existing glossary terms, map them.
    If not, suggest new glossary entries for all the columns don't leave any columns or fields which are passed to you.
    Return structured JSON with:
    - mapped_columns
    - new_glossary_entries
    - new_domain (if applicable)
    - semantic_tags (per column)
    """

    load_dotenv()

    llm = init_chat_model("anthropic:claude-3-5-sonnet-latest")
    response = llm.invoke(prompt)
    return {"messages": [{"role": "user", "content": response.content}]}
