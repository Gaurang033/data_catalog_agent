# utils/semantic_tagger.py
from typing import Dict, List
import re
import pandas as pd

# Define basic semantic type patterns
tag_rules = {
    "PII": [
        r"email", r"phone", r"ssn", r"social.*security", r"dob", r"birth.*date"
    ],
    "Date": [
        r"date", r"day", r"dob", r"timestamp"
    ],
    "Identifier": [
        r"id$", r"_id$", r"uuid", r"identifier"
    ],
    "Metric": [
        r"amount", r"total", r"score", r"count", r"volume", r"rate"
    ],
    "Geo": [
        r"country", r"state", r"city", r"zip", r"location"
    ],
    "Contact": [
        r"email", r"phone", r"contact"
    ]
}

def infer_semantic_tags(schema: Dict[str, str], sample_df: pd.DataFrame) -> Dict[str, List[str]]:
    tagged = {}
    for column in schema:
        tags = []
        lower_col = column.lower()
        for tag, patterns in tag_rules.items():
            if any(re.search(pat, lower_col) for pat in patterns):
                tags.append(tag)
        tagged[column] = tags or ["Unknown"]
    return tagged
