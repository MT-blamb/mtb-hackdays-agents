from pathlib import Path
import asyncio

from strands import Agent
from strands.models import BedrockModel

from mcp.client.stdio import stdio_client
from mcp.client.registry import ClientRegistry
from mcp.client.server_parameters import StdioServerParameters

# -------- Bedrock model (same as your working baseline) --------

BEDROCK_MODEL_ID = "apac.anthropic.claude-3-sonnet-20240229-v1:0"

bedrock_model = BedrockModel(
    model_id=BEDROCK_MODEL_ID,
    temperature=0.2,
    region_name="ap-northeast-1",
)

# -------- MCP server config: local Athena server --------

REPO_ROOT = Path(__file__).resolve().parents[1]

athena_server = StdioServerParameters(
    command="python",
    args=[str(REPO_ROOT / "scenario3_custom_server" / "mtb_athena_server.py")],
)

registry = ClientRegistry()


async def setup_mcp_clients() -> dict[str, object]:
    """
    Start the Athena MCP server and register it in the registry.
    """
    client = await stdio_client(athena_server)
    session = await client.connect()
    await registry.register(session, "mtb_athena")
    return {"mtb_athena": session}


SYSTEM_PROMPT = """
You are a data assistant for Moneytree.

You can use an MCP tool called 'mtb_athena' which exposes Athena as tools:
- list_tables(database=None) -> list of table names
- describe_table(database, table) -> list of columns (name, type, comment)
- run_readonly_query(database, sql, max_rows=50) -> list of rows as dicts

Use these tools whenever the user asks questions that require inspecting data
(e.g. top merchants by monthly volume, counts, aggregations, filters).

Rules:
- Only run read-only SELECT queries (the tool will reject anything else).
- Prefer limiting to at most 50 rows and summarising results.
- When unsure of schema, first call describe_table() before writing SQL.
- For demo prompts like "top 10 merchants by monthly subscription volume",
  write SQL that makes reasonable assumptions about column names (e.g.
  merchant, amount, transaction_date, etc.) based on describe_table().
"""


async def run_agent_repl():
    # 1) Start MCP server and register it
    await setup_mcp_clients()

    # 2) Create the Strands agent with Bedrock + MCP registry
    agent = Agent(
        model=bedrock_model,
        system_prompt=SYSTEM_PROMPT,
        mcp_registry=registry,
    )

    print("MTB Athena agent ready. Ask me a question about your Athena data.")
    print("Example: 'Show me the top 10 merchants by monthly subscription volume in the last 3 months.'")
    print("Press Ctrl+C to exit.\n")

    while True:
        try:
            user_input = input("You: ")
            if not user_input.strip():
                continue
            response = agent(user_input)
            print("\nAgent:\n", response, "\n")
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break


def main():
    asyncio.run(run_agent_repl())


if __name__ == "__main__":
    main()
