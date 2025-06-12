import json
import re
from typing import Annotated,Literal

from anthropic import Anthropic
from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from typing_extensions import TypedDict

from nodes.map_business_glossary import (GlossaryInput, map_glossary,
                                         map_glossary_node)

# load_dotenv()
# client = Anthropic()
#
# load_dotenv()
# llm = init_chat_model("anthropic:claude-3-5-sonnet-latest")


class State(TypedDict):
    messages: Annotated[list, add_messages]
    file_path: str
    domain: str
    source_type: Literal["csv", "webpage", "swagger"]


# Step 4: Build LangGraph flow
def build_graph():
    graph_builder = StateGraph(State)
    graph_builder.add_node("map_glossary", map_glossary_node)
    graph_builder.add_edge(START, "map_glossary")
    graph_builder.add_edge("map_glossary", END)
    return graph_builder.compile()

MAPPED_COLUMNS_FILE = "output/mapped_columns.json"
NEW_GLOSSARY_FILE = "output/new_glossary_entries.json"
NEW_DOMAIN_FILE = "output/new_domains.json"


# Step 3: Save output to three files
def save_results(data: dict):
    with open(MAPPED_COLUMNS_FILE, "w") as f:
        json.dump(data.get("mapped_columns", {}), f, indent=2)

    with open(NEW_GLOSSARY_FILE, "w") as f:
        json.dump(data.get("new_glossary_entries", {}), f, indent=2)

    with open(NEW_DOMAIN_FILE, "w") as f:
        json.dump(data.get("new_domain", {}), f, indent=2)

    return "DONE"


def extract_json_block(text: str) -> dict:
    try:
        matches = re.findall(r"\{(?:[^{}]|(?R))*\}", text, re.DOTALL)
        merged = {}
        for match in matches:
            try:
                parsed = json.loads(match)
                for k in parsed:
                    if isinstance(parsed[k], dict):
                        merged[k] = {**merged.get(k, {}), **parsed[k]}
                    else:
                        merged[k] = parsed[k]
            except Exception:
                continue
        return merged
    except Exception as e:
        print(f"❌ Error parsing JSON chunks: {e}")
        print("Raw message:\n", text)
        return {}

# Run it all
def main(file_path: str, domain: str, source_type: str):
    graph = build_graph()
    state = graph.invoke({"file_path": file_path, "domain": domain, "source_type": source_type})
    print(state["messages"][-1].content)
    final_message = state["messages"][-1].content

    parsed_output = extract_json_block(final_message)
    if parsed_output:
        save_results(parsed_output)



    save_results(parsed_output)

    # from pprint import pprint

    # pprint(state)


if __name__ == "__main__":
    # Example usage
    # main("data/raw_pos.csv", "sales", "csv")
    main("https://developers.facebook.com/docs/graph-api/reference/user/", "sales", "webpage")
    # main("https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28", "sales", "webpage")

    # main("https://petstore.swagger.io/v2/swagger.json", "sales", "swagger")

