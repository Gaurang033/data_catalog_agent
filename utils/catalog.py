import json
from pathlib import Path
from typing import Any

# Define base directory for mapping files
MAPPING_DIR = Path(__file__).parent.parent / "mappings"


def load_json(file_name: str) -> dict:
    file_path = MAPPING_DIR / file_name
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_business_glossary(domain: str) -> dict[str, Any]:
    glossary = load_json("glossary.json")
    return glossary.get(domain, {})


def get_owner(domain: str) -> str:
    owners = load_json("owner.json")
    return owners.get(domain, "Unknown Owner")


def get_steward(domain: str) -> str:
    stewards = load_json("stewards.json")
    return stewards.get(domain, "Unknown Steward")
