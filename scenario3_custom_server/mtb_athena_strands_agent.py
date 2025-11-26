# scenario3_custom_server/mtb_athena_strands_agent.py

import os
import sys

from mcp import StdioServerParameters
from mcp.client.stdio import stdio_client

from strands.agent import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient

DEFAULT_DB = "lakehouse_experimental_jp_production"


def build_bedrock_model() -> BedrockModel:
    """
    Configure the Bedrock model that Strands uses.
    Uses Haiku by default (works with on-demand in ap-northeast-1).

    You can override with:
      export MTB_BEDROCK_MODEL_ID="your-model-id"
      export MTB_BEDROCK_INFERENCE_PROFILE_ARN="arn:aws:bedrock:ap-northeast-1:...:inference-profile/..."
    """
    model_id = os.getenv(
        "MTB_BEDROCK_MODEL_ID",
        "anthropic.claude-3-haiku-20240307-v1:0",
    )

    inference_profile_arn = os.getenv("MTB_BEDROCK_INFERENCE_PROFILE_ARN")
    additional_request_fields = {}

    if inference_profile_arn:
        # Avoid the “on-demand throughput isn’t supported” error
        additional_request_fields["inferenceConfig"] = {
            "inferenceProfileArn": inference_profile_arn
        }

    # NOTE: do NOT pass `region` here; strands BedrockModel doesn’t accept it.
    return BedrockModel(
        model_id=model_id,
        temperature=0.1,
        max_tokens=2000,
        additional_request_fields=additional_request_fields,
    )


def main() -> None:
    print("Starting Athena MCP server...")

    # Start the Athena MCP server as a subprocess over stdio.
    # We pass through the current environment so AWS creds & region are visible.
    server_params = StdioServerParameters(
        command=sys.executable,
        args=["scenario3_custom_server/mtb_athena_server.py"],
        env=dict(os.environ),
    )

    # MCPClient hides the async MCP plumbing and gives us Strands-ready tools.
    client = MCPClient(lambda: stdio_client(server_params))

    # This context manager:
    #   - starts the server
    #   - establishes the MCP session
    #   - handles initialize/list_tools/call_tool under the hood
    with client:
        print("MCP session established. Loading tools from Athena server...")
        tools = client.list_tools_sync()

        print(f"Loaded {len(tools)} MCP tools:")
        for t in tools:
            tool_name = getattr(t, "tool_name", None)
            if tool_name is None:
                tool_name = getattr(t, "__name__", None) or repr(t)
            print(f"  - {tool_name}")

        system_prompt = f"""
You are the Moneytree Athena assistant.

ENVIRONMENT
- You talk to AWS Athena via MCP tools.
- The default database is `{DEFAULT_DB}`.
- You are used interactively by engineers and analysts for ad-hoc questions.

TOOLS (BEHIND THE SCENES)
- list_tables(database?): list tables in an Athena database.
- describe_table(database, table): inspect schema.
- run_readonly_query(database, sql, max_rows): run SELECT-only queries.

HARD SAFETY RULES
- You MUST keep queries read-only: only SELECT / SHOW / DESCRIBE.
- NEVER use INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, or TRUNCATE.
- If the user asks you to modify data, explain that you are read-only and
  propose a read-only diagnostic query instead.

GENERAL BEHAVIOR
- Think like a data engineer who does exploratory analysis.
- Be explicit about what tables you used and why.
- Always:
  1) Reason about which tables are relevant.
  2) Build SQL that answers the question.
  3) Execute it with run_readonly_query.
  4) Explain the result in concise, business-friendly language.
- If you hit a timeout or error:
  - Fall back to a smaller query (e.g., add LIMIT, or narrow WHERE).
  - Explain what you tried and what the limitation is.

SCHEMA DISCOVERY & RELATIONSHIPS
- When a question involves business concepts (wifi, salary, subscriptions,
  accounts, guests, etc.), first explore the schema:

  1) Call list_tables on `{DEFAULT_DB}` to discover candidate tables.
     - Look for table name patterns like:
       - transactions, accounts, users, guests, subscriptions, salary, etc.
  2) For the most relevant tables, call describe_table to see:
     - key columns (id, account_id, user_id, guest_id, subscription_id, etc.)
     - amounts, timestamps, descriptions, statuses.
  3) Infer joins based on shared column names and likely semantics:
     - account_id across tables → probably joins on account_id.
     - user_id or guest_id across tables → join on that id.
     - subscription_id, claim_id, transaction_id, etc. as foreign keys.
  4) Prefer simple equi-joins:
     - ON t.account_id = a.account_id
     - ON t.user_id = u.user_id
  5) Start with small, safe queries:
     - LIMIT 5 or LIMIT 50.
     - Narrow WHERE clauses if the tables are large.

WIFI TRANSACTIONS (SPECIFIC EXAMPLE)
- There is NO dedicated "wifi_transactions" table.
- To find wifi-related transactions:
  - Use list_tables + describe_table to find the main transactions table.
  - In that table, search text columns such as:
    - description_guest, description_raw, description_pretty
  - Use patterns like:
      LOWER(COALESCE(description_guest, description_raw, description_pretty, ''))
        LIKE '%wi-fi%'
      OR LOWER(...) LIKE '%wifi%'
      OR LOWER(...) LIKE '%wi fi%'
  - LIMIT 5 or 50 for samples.

JOINS FOR RICHER QUESTIONS
- When the user asks questions like:
  - "How many unique guests bought X and when?"
  - "Which accounts have recurring wifi charges?"
  - "Link transactions to subscription metadata."
- Do NOT just stay in a single table if that is obviously insufficient.
- Instead:
  1) Identify the fact table (often `transactions` or something similar).
  2) Identify dimension/lookup tables (accounts, guests, subscriptions, etc.).
  3) Look for shared keys using describe_table on those tables.
  4) Propose a JOIN:
       SELECT ...
       FROM transactions t
       JOIN accounts a ON t.account_id = a.account_id
       ...
     with appropriate WHERE filters and LIMIT.
  5) Run a small version of the query first. If it works, summarize it.
- If you genuinely cannot find a good join path, say that explicitly and
  list the tables/columns you inspected.

RESPONSE FORMAT (VERY IMPORTANT)
For each user question that leads to a query, respond in this structure:

1) Brief reasoning (1–3 sentences max), e.g.:
   "I'll query the transactions table and filter on wifi keywords in the
    description_guest field."

2) Show the FINAL SQL you actually decided to run in a fenced SQL block:
   ```sql
   SELECT ...
   FROM ...
   WHERE ...
   LIMIT ...
3) Brief summary of the results in plain language:

- Key counts, date ranges, interesting patterns.

- Mention which tables and join keys were used.

4) Only if helpful, show a small tabular-style text sample of rows
(but do NOT dump huge JSON blobs).

If a question can be answered from earlier context (previous queries and
descriptions you already saw), you can reuse that knowledge without
re-running tools, but still show an example SQL that would answer it.

In Athena, identifiers that start with a number (or have other “weird” characters) must be quoted. 
Unquoted identifiers have to start with a letter.

So for a table like 250911_ai459_pbo_salary_capture_dataset_complete_gold
, you must use quotes:
```sql
SELECT *
FROM "250911_ai459_pbo_salary_capture_dataset_complete_gold"
WHERE ...
```

WHEN YOU ARE UNSURE

Be honest if you cannot find the right tables or joins.

Describe what you tried: which tables you listed, which schemas you inspected,
and why it might not be possible without additional domain knowledge.
"""
          # Build the Strands Agent with:
          #   - our Bedrock model
          #   - tools loaded from the MCP server
          #   - the Athena-specific system prompt
        model = build_bedrock_model()
        agent = Agent(
              model=model,
              system_prompt=system_prompt,
              tools=tools,
          )

        print("\n🚀 Athena Strands Agent Ready!")
        print("Type questions about your data, for example:")
        print("  • 'Show me 5 wifi transactions'")
        print("  • 'Which tables mention salary?'")
        print("  • 'Describe the transactions table'")
        print("  • 'Generate SQL to find negative transactions in transactions'")
        print("Type 'exit' to quit.\n")

        # Simple sync REPL
        while True:
            try:
                user_input = input("💬 Ask: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting…")
                break

            if not user_input:
                continue
            if user_input.lower() in ("exit", "quit"):
                break

            try:
                # Print a header, then let Strands stream the answer.
                print("\n🤖 LLM Answer:\n")
                _ = agent(user_input)  # we don't need to print `result`
                print()  # just a blank line for spacing
            except Exception as e:
                print(f"\n❌ Error: {e}\n")

if __name__ == "__main__": 
    main()