# import pandas as pd


# def get_schema_and_sample_date(file_path: str, sample_size=10):
#     try:
#         df = pd.read_csv(file_path, nrows=sample_size)
#         schema = dict(df.dtypes.apply(lambda dt: dt.name))
#         return df, schema

#     except Exception as e:
#         print(f"Error reading CSV file: {e}")
#         return {}

# utils/source_parser.py

import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import yaml
import re
import PyPDF2
import speech_recognition as sr


def guess_type(value: str) -> str:
    if re.match(r"^\d+$", value):
        return "integer"
    elif re.match(r"^\d+\.\d+$", value):
        return "float"
    elif re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return "date"
    return "string"


def parse_pdf(file_path: str) -> str:
    try:
        text = ""
        with open(file_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text() or ""
        fields = {}
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        i = 0
        while i < len(lines) - 1:
            if lines[i].startswith("Field Name:") and lines[i + 1].startswith(
                "Description:"
            ):
                field_name = lines[i].replace("Field Name:", "").strip()
                description = lines[i + 1].replace("Description:", "").strip()
                fields[field_name] = description
                i += 2
            else:
                i += 1

        return fields, "Parsed from PDF file"
    except Exception as e:
        print(f"Error reading PDF: {e}")
    return {}, "PDF parsing failed"


def parse_audio(file_path: str):
    recognizer = sr.Recognizer()
    try:
        with sr.AudioFile(file_path) as source:
            audio = recognizer.record(source)
        text = recognizer.recognize_google(audio)
        fields = {}

        # Naively parse: Assume format "Field: Description" in the transcript
        lines = text.splitlines()
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                fields[key.strip()] = value.strip()
        return fields, "Parsed from audio transcription"

    except sr.UnknownValueError:
        print("Speech Recognition could not understand audio")
        return {}, "Speech not understood"
    except sr.RequestError as e:
        print(f"Speech Recognition service error: {e}")
        return {}, "Speech recognition request failed"
    except Exception as e:
        print(f"Error processing audio file: {e}")
        return {}, "Audio parsing failed"


def parse_csv(file_path: str, sample_size=10):
    try:
        df = pd.read_csv(file_path, nrows=sample_size)
        schema = dict(df.dtypes.apply(lambda dt: dt.name))
        return schema, df
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return {}, pd.DataFrame()


def parse_excel(file_path: str, sample_size=10):
    try:
        df = pd.read_excel(file_path, nrows=sample_size)
        schema = dict(df.dtypes.apply(lambda dt: dt.name))
        return schema, df
    except Exception as e:
        print(f"Error reading Excel: {e}")
        return {}, pd.DataFrame()


def parse_parquet(file_path: str, sample_size=10):
    try:
        df = pd.read_parquet(file_path)
        df_sample = df.head(sample_size)
        schema = dict(df_sample.dtypes.apply(lambda dt: dt.name))
        return schema, df_sample
    except Exception as e:
        print(f"Error reading Parquet: {e}")
        return {}, pd.DataFrame()


def parse_json(file_path: str, sample_size=10):
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            df = pd.DataFrame(data)
            df_sample = df.head(sample_size)
            schema = dict(df_sample.dtypes.apply(lambda dt: dt.name))
            return schema, df_sample
    except Exception as e:
        print(f"Error reading JSON: {e}")
        return {}, pd.DataFrame()


def parse_api(url: str, sample_size=10):
    try:
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        df_sample = df.head(sample_size)
        schema = dict(df_sample.dtypes.apply(lambda dt: dt.name))
        return schema, df_sample
    except Exception as e:
        print(f"Error reading API response: {e}")
        return {}, pd.DataFrame()


def parse_webpage(url: str):
    try:
        response = requests.get(url)
        # data = response.headers
        # print("response :",data)
        soup = BeautifulSoup(response.text, "html.parser")
        # print("soup :",soup)

        fields = {}
        # Very simple logic: look for <code> blocks and treat short entries as field names
        seen = set()

        # Table-based extraction of field + description
        for row in soup.select("table tr"):
            cols = row.find_all("td")
            if len(cols) >= 2:
                field = cols[0].text.strip()
                desc = cols[1].text.strip()
                if field and len(field) < 50 and field not in seen:
                    seen.add(field)
                    fields[field] = desc

        # Fallback to <code> tags if no descriptions found
        for code in soup.find_all("code"):
            field = code.text.strip()
            if 1 < len(field) < 50 and " " not in field and field not in seen:
                seen.add(field)
                fields[field] = ""
        print("--------fields--------------", fields)
        return fields, "Webpage input — parsed fields and fallback"

        # return fields, "Webpage input — no sample data"

    except Exception as e:
        print(f"Error parsing webpage: {e}")
        return {}, "Webpage parsing failed"


def parse_columns_only(file_path: str, definitions_path: str = None):
    with open(file_path) as f:
        columns = json.load(f)
    definitions = {}
    if definitions_path:
        with open(definitions_path) as df:
            definitions = json.load(df)
    return columns, pd.DataFrame(), definitions


def parse_swagger(url_or_path: str):
    try:
        if url_or_path.startswith("http"):
            response = requests.get(url_or_path)
            swagger = response.json()
        else:
            with open(url_or_path, "r") as f:
                swagger = (
                    json.load(f) if url_or_path.endswith(".json") else yaml.safe_load(f)
                )

        fields = {}
        info = "Swagger schema parsed"

        # Swagger v2
        paths = swagger.get("paths", {})
        for path, methods in paths.items():
            for method, details in methods.items():
                if method.lower() in ["post", "get"]:
                    # Check for schema in requestBody (OpenAPI 3) or parameters (Swagger 2)
                    request_body = details.get("requestBody", {})
                    content = request_body.get("content", {})
                    schema = content.get("application/json", {}).get("schema", {})

                    if not schema and "parameters" in details:
                        for param in details["parameters"]:
                            if param.get("in") == "body":
                                schema = param.get("schema", {})
                                break

                    # Resolve $ref if present
                    if "$ref" in schema:
                        ref_path = schema["$ref"].split("/")[-1]
                        schema = (
                            swagger["definitions"][ref_path]
                            if "definitions" in swagger
                            else swagger["components"]["schemas"][ref_path]
                        )

                    props = schema.get("properties", {})
                    for name, prop in props.items():
                        field_type = prop.get("type", "string")
                        fields[name] = field_type

                    return fields, info

        return {}, "No suitable schema found in Swagger"

    except Exception as e:
        return {}, f"Swagger parsing failed: {e}"
