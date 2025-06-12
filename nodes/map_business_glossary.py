from dotenv import load_dotenv
from langchain.chat_models import init_chat_model

from utils.catalog import get_owner, get_steward, get_business_glossary
from utils.source_parser import (
    parse_csv,
    parse_webpage,
    parse_swagger,
    parse_excel,
    parse_parquet,
    parse_json,
    parse_api,
)
from utils.semantic_tagger import infer_semantic_tags
from typing import TypedDict, Literal
from pandas import DataFrame
import json


class GlossaryInput(TypedDict):
    schema: dict
    sample_data: DataFrame
    owner: str
    steward: str
    domain: str
    glossary: dict
    semantic_tags: dict


def get_schema_and_sample_data(
    source_type: Literal[
        "csv", "webpage", "swagger", "excel", "parquet", "json", "api"
    ],
    path: str,
):
    if source_type == "csv":
        return parse_csv(path)
    elif source_type == "webpage":
        return parse_webpage(path)
    elif source_type == "swagger":
        return parse_swagger(path)
    elif source_type == "excel":
        return parse_excel(path)
    elif source_type == "parquet":
        return parse_parquet(path)
    elif source_type == "json":
        return parse_json(path)
    elif source_type == "api":
        return parse_api(path)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def chunk_dict(d, chunk_size):
    keys = list(d.keys())
    for i in range(0, len(keys), chunk_size):
        yield {k: d[k] for k in keys[i : i + chunk_size]}


def map_glossary(file_path: str, domain: str, source_type: str) -> GlossaryInput:
    schema, sample = get_schema_and_sample_data(source_type, file_path)
    # print("****_______*********schema_______----------",schema)
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
        "semantic_tags": tags,
    }


def map_glossary_node(state: dict) -> dict:
    file_path = state["file_path"]
    domain = state["domain"]
    source_type = state["source_type"]

    glossary_context = map_glossary(file_path, domain, source_type)
    schema_chunks = list(chunk_dict(glossary_context["schema"], chunk_size=50))

    load_dotenv()
    llm = init_chat_model("anthropic:claude-3-5-sonnet-latest")

    responses = []
    i = 0

    for chunk in schema_chunks:
        print("_______________chunk____________________", chunk)

        chunk_tags = {k: glossary_context["semantic_tags"].get(k, []) for k in chunk}
        chunk_sample = (
            glossary_context["sample_data"][list(chunk.keys())]
            .head()
            .to_dict(orient="list")
        )

        prompt = f"""
        You are a highly skilled data catalog assistant.Analyze this chunk of fields from a {source_type}

        Context:
        The data you are analyzing is likely from a **social media company**. The source is a {source_type} file.
        Your task is to map all raw schema fields to business glossary terms based on their names,values, description, types, and any semantic hints available.

        Inputs:
        - Domain: {glossary_context["domain"]}
        - Owner: {glossary_context["owner"]}
        - Steward: {glossary_context["steward"]}

        Schema:
        {json.dumps(chunk, indent=2)}

        Sample Data:
        {glossary_context["sample_data"]}

        Glossary:
        {json.dumps(glossary_context["glossary"], indent=2)}

        Semantic Tags:
        {json.dumps(chunk_tags, indent=2)}

        Instructions:
        - Match fields from the schema/fields to glossary terms if names or types match directly or closely.
        - If no match is found, suggest a new glossary entry with appropriate business term, value, description, definition (inferred from field name and context), data type, and example.
        - Output should include semantic tags for each field.
        - In output i don't want only some fields i want output for each and every field that is passed to you so print the output for 
        each and every field according to the logic.

        If columns match existing glossary terms, map them.
        If not, suggest new glossary entries for all the columns—do not skip any.
        Return structured JSON with:
        - mapped_columns
        - new_glossary_entries
        - new_domain (if applicable)
        - semantic_tags (per column)
        """

        result = llm.invoke(prompt)
        responses.append(result.content)

    full_output = "\n".join(responses)
    return {"messages": [{"role": "user", "content": full_output}]}
