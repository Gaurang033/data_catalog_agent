import json
from typing import Annotated

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


# Step 4: Build LangGraph flow
def build_graph():
    graph_builder = StateGraph(State)
    graph_builder.add_node("map_glossary", map_glossary_node)
    graph_builder.add_edge(START, "map_glossary")
    graph_builder.add_edge("map_glossary", END)
    return graph_builder.compile()


# Run it all
def main(file_path: str, domain: str):
    graph = build_graph()
    state = graph.invoke({"file_path": file_path, "domain": domain})
    print(state["messages"][-1].content)
    final_message = state["messages"][-1].content

    from pprint import pprint

    pprint(state)


if __name__ == "__main__":
    # Example usage
    main("data/raw_pos.csv", "sales")


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
