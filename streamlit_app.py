# streamlit_app.py  (Athena + MCP + Strands + Bedrock)

import os
import re
import sys

import traceback
import pandas as pd
import streamlit as st

from mcp import StdioServerParameters
from mcp.client.stdio import stdio_client

from strands.agent import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient

from pathlib import Path
from strands.types.exceptions import MCPClientInitializationError



# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------

# New default DB you requested
DEFAULT_DB = os.getenv(
    "MTB_ATHENA_DEFAULT_DB",
    "lakehouse_omoikane_streaming_jp_production",
)


# --------------------------------------------------------------------
# Bedrock model helper
# --------------------------------------------------------------------

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
        # Avoid the “on-demand throughput isn’t supported” error for some models
        additional_request_fields["inferenceConfig"] = {
            "inferenceProfileArn": inference_profile_arn
        }

    # NOTE: do NOT pass `region` here; BedrockModel doesn’t accept it.
    return BedrockModel(
        model_id=model_id,
        temperature=0.1,
        max_tokens=2000,
        additional_request_fields=additional_request_fields,
    )

# --------------------------------------------------------------------
# Helpers for displaying agent output
# --------------------------------------------------------------------

def extract_sql_block(text: str) -> str | None:
    """
    Extract the first ```sql ... ``` block from the given text.
    Returns just the SQL body, or None if no SQL block is found.
    """
    if not text:
        return None

    match = re.search(
        r"```sql\s*(.*?)```",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return None

    sql_body = match.group(1).strip()
    return sql_body or None


def parse_markdown_table_into_df(text: str) -> pd.DataFrame | None:
    """
    Look for the first markdown table in the text and return it as a DataFrame.

    Example it can parse:

    Company Name | Avg Total Compensation
    --- | ---
    Rakuten Card | 998,080,550
    SMBC Card    | 870,562,703
    """
    lines = text.splitlines()
    start = None
    end = None

    # Find header line (starts with | and has at least one more |)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("|") and "|" in stripped[1:]:
            # Require next line to also look like a table row
            if i + 1 < len(lines) and lines[i + 1].strip().startswith("|"):
                start = i
                break

    if start is None:
        return None

    # Find the end (first non-table line after start)
    for j in range(start + 1, len(lines)):
        if not lines[j].strip().startswith("|"):
            end = j
            break

    if end is None:
        end = len(lines)

    table_lines = [ln.strip().strip("|") for ln in lines[start:end]]
    if len(table_lines) < 2:
        return None

    header_cells = [c.strip() for c in table_lines[0].split("|")]

    data_rows: list[list[str]] = []
    for row in table_lines[1:]:
        cells = [c.strip() for c in row.split("|")]

        # Skip separator rows like "---" or ":---:"
        if all(set(c) <= {"-", ":"} for c in cells if c):
            continue

        if len(cells) != len(header_cells):
            continue

        data_rows.append(cells)

    if not data_rows:
        return None

    df = pd.DataFrame(data_rows, columns=header_cells)

    # Try to convert numeric columns
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="ignore")

    return df


def parse_numbered_list_into_df(text: str) -> pd.DataFrame | None:
    """
    Parse lines like:
    1. Rakuten Card (998,080,550 transactions)
    2. SMBC Card (870,562,703 transactions)

    into a DataFrame with columns: label, value.
    """
    pattern = r"^\s*\d+\.\s*([^(]+?)\s*\(([\d,]+)\s+\w+"
    rows: list[dict[str, object]] = []

    for line in text.splitlines():
        m = re.match(pattern, line)
        if not m:
            continue

        label = m.group(1).strip()
        num_str = m.group(2).replace(",", "")
        try:
            value = int(num_str)
        except ValueError:
            continue

        rows.append({"label": label, "value": value})

    if not rows:
        return None

    return pd.DataFrame(rows)


# --------------------------------------------------------------------
# Agent / MCP wiring (init_agent)
# --------------------------------------------------------------------

def init_agent():
    # Reuse existing agent if we already have one
    if "agent" in st.session_state and st.session_state["agent"] is not None:
        return st.session_state["agent"]

    # Also avoid restarting if we know initialization failed earlier
    if st.session_state.get("agent_init_failed"):
        return None

    st.write("🔌 Initializing Athena MCP client & Strands agent…")

    # Resolve path to the server script relative to this file
    here = os.path.dirname(os.path.abspath(__file__))
    server_script = os.path.join(here, "scenario3_custom_server", "mtb_athena_server.py")

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[server_script],
        env=dict(os.environ),
    )

    client = MCPClient(lambda: stdio_client(server_params))

    try:
        client.start()  # same as client.__enter__(), but explicit
        tools = client.list_tools_sync()

        model = build_bedrock_model()

        system_prompt = f"""
You are the Moneytree Athena assistant.

ENVIRONMENT
- You talk to AWS Athena via MCP tools.
- The default database is `{DEFAULT_DB}` (unqualified table names refer here).
- You are used interactively by engineers and analysts for ad-hoc questions.

AVAILABLE TOOLS (BEHIND THE SCENES)
- list_tables(database?): list tables in an Athena database.
- describe_table(database, table): inspect schema.
- run_readonly_query(database, sql, max_rows): run SELECT-only queries.

HARD SAFETY RULES
- You MUST keep queries read-only: only SELECT / SHOW / DESCRIBE.
- NEVER use INSERT, UPDATE, DELETE, MERGE, CREATE, DROP, ALTER, TRUNCATE, GRANT, or REVOKE.
- If the user asks you to modify data, explain that you are read-only and
  propose a read-only diagnostic query instead.

ATHENA IDENTIFIERS
- In Athena, identifiers that start with a number (or have other "weird" characters) must be quoted.
- Unquoted identifiers must start with a letter.
- For example, table 250911_ai459_pbo_salary_capture_dataset_complete_gold MUST be referenced as:
```sql
  SELECT *
  FROM "250911_ai459_pbo_salary_capture_dataset_complete_gold"
  WHERE ...
```

GENERAL BEHAVIOR
* Think like a data engineer doing exploratory analysis.
* Be explicit about what tables you used and why.
* For any question that depends on actual data:
  1. Reason about which tables are relevant.
  2. Build SQL that answers the question.
  3. EXECUTE that SQL via run_readonly_query (do not just propose it and stop).
  4. Explain the result in concise, business-friendly language.
* If you hit a timeout or error:
  * Try a smaller query (e.g., add LIMIT, or narrow WHERE conditions).
  * If it still fails, clearly say what you attempted and include the SQL you tried.
  * Do NOT fabricate result rows or numeric values if the query did not succeed.

SCHEMA DISCOVERY & RELATIONSHIPS
* When a question involves business concepts (wifi, salary, subscriptions,
  accounts, guests, institutions, etc.), first explore the schema:
  1. Call list_tables on `{DEFAULT_DB}` to discover candidate tables.
     * Look for table name patterns like:
       * transactions, accounts, users, guests, subscriptions, salary, institutions, etc.
  2. For the most relevant tables, call describe_table to see:
     * key columns (id, account_id, user_id, guest_id, credential_id, institution_id, subscription_id, etc.)
     * amounts, timestamps, descriptions, statuses.
  3. Infer joins based on shared column names and likely semantics:
     * account_id across tables → JOIN on account_id.
     * user_id or guest_id across tables → JOIN on that id.
     * credential_id, institution_id, subscription_id, claim_id, transaction_id, etc. as foreign keys.
  4. Prefer simple equi-joins:
     * ON t.account_id = a.id
     * ON a.credential_id = c.id
     * ON c.institution_id = i.id
  5. Start with small, safe queries:
     * LIMIT 5 or LIMIT 50.
     * Add WHERE clauses if the tables are large.

WIFI TRANSACTIONS (SPECIFIC EXAMPLE)
* There is NO dedicated "wifi_transactions" table.
* To find wifi-related transactions:
  * Use list_tables + describe_table to find the main transactions table.
  * In that table, search text columns such as:
    * description_guest, description_raw, description_pretty
  * Use patterns like:
    LOWER(COALESCE(description_guest, description_raw, description_pretty, '')) LIKE '%wi-fi%'
    OR LOWER(...) LIKE '%wifi%'
    OR LOWER(...) LIKE '%wi fi%'
  * LIMIT 5 or 50 for samples.

JOINS FOR RICHER QUESTIONS
* For questions like:
  * "How many unique guests bought X and when?"
  * "Which institutions have the highest number of transactions?"
  * "Link transactions to subscription metadata."
* Do NOT stay in a single table if that is obviously insufficient.
* Instead:
  1. Identify the fact table (often `transactions` or similar).
  2. Identify dimension / lookup tables (accounts, credentials, institutions, subscriptions, etc.).
  3. Look for shared keys using describe_table on those tables.
  4. Propose and RUN a JOIN, e.g.:
```sql
     SELECT i.name, COUNT(*) AS txn_count
     FROM transactions t
     JOIN accounts a      ON t.account_id = a.id
     JOIN credentials c   ON a.credential_id = c.id
     JOIN institutions i  ON c.institution_id = i.id
     GROUP BY i.name
     ORDER BY txn_count DESC
     LIMIT 5;
```
  5. Run a small version of the query first (LIMIT) and summarize the results.

RESPONSE FORMAT (VERY IMPORTANT)
For each user question that leads to a query, your answer MUST follow this structure:

1. **Short answer / reasoning** (2–5 sentences max)
   * Explain briefly what you did:
     * Which tables you chose and why.
     * What filters/joins you applied.
     * What the high-level result means for the user.

2. **SQL I ran** (exactly ONE fenced block with the FINAL SQL you executed)
   * Use a single ```sql block containing ONLY the final query:
```sql
   SELECT ...
   FROM ...
   WHERE ...
   GROUP BY ...
   ORDER BY ...
   LIMIT ...
```
   * This must be the query actually sent to run_readonly_query.
   * If you tried multiple queries internally, only show the final, best one.

3. **Result summary**
   * Summarize key figures in natural language:
     * Counts, top N entities, date ranges, interesting patterns, etc.
   * Mention which tables and join keys were used (e.g., "joined transactions→accounts→credentials→institutions via account_id, credential_id, institution_id").

4. **Tabular snippet for UI / charts (when appropriate)**
   * For "top N" / grouped / aggregated questions:
     * Return a small **markdown table** (ideal for 5–20 rows, up to ~5 columns).
     * Example:
       | institution_name | txn_count |
       | ---------------- | --------- |
       | Rakuten Card     | 998080550 |
       | SMBC Card        | 870562703 |
   * Use clear, machine-friendly column names (no emojis in headers).
   * This table will be used by the UI to build charts, so ensure:
     * Numbers are plain numeric values (no commas or currency symbols).
     * String labels go in their own column.

5. **Raw samples (optional)**
   * Only if helpful, show 3–5 example rows (e.g. for "show sample wifi transactions").
   * Keep it small and readable; do not dump huge JSON blobs.

TOOL USAGE CONTRACT
* When you need Athena data, you MUST:
  * Call run_readonly_query with the SQL you decide is best.
  * Base your numeric answers on its result rows.
* If run_readonly_query fails or times out:
  * Say clearly that the query failed and why (if known).
  * Show the SQL you attempted in the ```sql block.
  * Do NOT fabricate rows or numeric aggregates.

WHEN YOU ARE UNSURE
* Be honest if you cannot find the right tables or joins.
* Describe what you tried:
  * Which tables you listed.
  * Which schemas you inspected.
  * Why it might not be possible without additional domain knowledge.
* When you are uncertain, prefer smaller, safer queries and clearly label any assumptions.
"""

        agent = Agent(
            model=model,
            system_prompt=system_prompt,
            tools=tools,
        )

        # Only store these if everything succeeded
        st.session_state["agent"] = agent
        st.session_state["mcp_client"] = client
        st.session_state["agent_init_failed"] = False
        return agent

    except Exception as e:
        st.session_state["agent"] = None
        st.session_state["mcp_client"] = None
        st.session_state["agent_init_failed"] = True
        st.session_state["agent_init_error"] = traceback.format_exc()

        st.error("Failed to start Athena MCP server. See details below:\n")
        st.code(st.session_state["agent_init_error"], language="text")
        return None

# --------------------------------------------------------------------
# Streamlit UI
# --------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title="Moneytree Athena Assistant",
        page_icon="🪙",
        layout="wide",
    )

    st.title("🪙 Moneytree Athena Assistant")
    st.caption(
        f"Query *read-only* Athena data from <span style='color: #32CD32; font-weight: bold;'>{DEFAULT_DB}</span> via an LLM + MCP + Strands. "
        "Backed by Amazon Bedrock; no data leaves AWS.",
        unsafe_allow_html=True
    )

    # ---------------- Sidebar: settings & example questions ----------------

    st.sidebar.title("⚙️ Settings")

    # Saved/example prompts
    example_questions = [
        "Show me 5 wifi transactions",
        "Which tables mention salary?",
        "Describe the transactions table",
        "Which institutions have the highest number of transactions? Show the top 5 with counts.",
        "Find the top 5 most generously paying companies from the salary capture table.",
    ]

    st.sidebar.subheader("💡 Example questions")
    selected_example = st.sidebar.selectbox(
        "Pick an example to load",
        options=["(none)"] + example_questions,
        index=0,
    )

    # SQL inspector toggle
    show_sql_inspector = st.sidebar.checkbox(
        "Show last SQL query",
        value=True,
        help="When enabled, shows the last SQL statement the agent decided to run.",
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "IAM & security:\n\n"
        "- Athena access is **read-only** (no INSERT/UPDATE/DELETE/DDL).\n"
        "- The app runs entirely against your AWS account & IAM role.\n"
        "- Bedrock keeps your data in-region; prompts & results stay in AWS."
    )

    # ---------------- Session state init ----------------

    if "agent" not in st.session_state:
        st.session_state["agent"] = None

    if "history" not in st.session_state:
        # each entry: {"question": str, "answer": str}
        st.session_state["history"] = []

    if "last_sql" not in st.session_state:
        st.session_state["last_sql"] = None

    if "init_error" not in st.session_state:
        st.session_state["init_error"] = None

    if "last_chart_df" not in st.session_state:
        st.session_state["last_chart_df"] = None

    if "chart_x_col" not in st.session_state:
        st.session_state["chart_x_col"] = None

    if "chart_y_col" not in st.session_state:
        st.session_state["chart_y_col"] = None

    # ---------------- Initialize agent (once) ----------------

    if st.session_state["agent"] is None and st.session_state["init_error"] is None:
        with st.spinner("🔌 Initializing Athena MCP client & Strands agent…"):
            try:
                agent = init_agent()
                st.session_state["agent"] = agent
            except Exception:
                # Capture full traceback so we don't keep retrying every rerun
                st.session_state["init_error"] = traceback.format_exc()

    if st.session_state["init_error"] is not None:
        st.error("Failed to start Athena MCP server / Strands agent.")
        st.exception(Exception(st.session_state["init_error"]))
        st.stop()

    agent = st.session_state["agent"]

    # ---------------- Main layout: input left, history + SQL right ----------------

    col_input, col_output = st.columns([1, 2])

    with col_input:
        st.subheader("📝 Ask a question")

        # If the user picked an example, pre-fill the text area
        default_question = ""
        if selected_example != "(none)":
            default_question = selected_example

        question = st.text_area(
            "Natural language question about your Athena data:",
            value=default_question,
            key="question_input",
            height=120,
            placeholder="e.g. Show me 5 wifi transactions in the last month",
        )

        ask_button = st.button("Ask Athena 💬", type="primary", use_container_width=True)

    with col_output:
        st.subheader("📜 Conversation")

        # Show past Q&A
        for item in st.session_state["history"]:
            st.markdown(f"**You:** {item['question']}")
            st.markdown(f"**Assistant:**\n\n{item['answer']}")
            st.markdown("---")

    # ---------------- Handle new question ----------------

    if ask_button and question.strip():
        with st.spinner("Thinking… running tools & queries against Athena…"):
            try:
                result = agent(question)
                answer_text = str(result).strip()
            except Exception as e:
                answer_text = f"❌ Error from agent: {e}"

        # Store in history
        st.session_state["history"].append(
            {"question": question, "answer": answer_text}
        )

        # Try to extract SQL from the answer & remember it
        sql = extract_sql_block(answer_text)
        if sql:
            st.session_state["last_sql"] = sql

        # Try to extract tabular data for charting:
        df = parse_markdown_table_into_df(answer_text)
        if df is None:
            df = parse_numbered_list_into_df(answer_text)

        if df is not None and not df.empty:
            st.session_state["last_chart_df"] = df

            # Pick default x/y columns (first col as label, first numeric col as value)
            numeric_cols = [
                c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])
            ]
            if numeric_cols:
                st.session_state["chart_x_col"] = df.columns[0]
                st.session_state["chart_y_col"] = numeric_cols[0]
            else:
                st.session_state["chart_x_col"] = None
                st.session_state["chart_y_col"] = None
        else:
            st.session_state["last_chart_df"] = None
            st.session_state["chart_x_col"] = None
            st.session_state["chart_y_col"] = None

        # Force a rerun so the right column shows the new entry immediately
        st.rerun()

    # ---------------- SQL Query Inspector ----------------

    if show_sql_inspector:
        st.markdown("---")
        st.subheader("🔍 Query Inspector (last SQL)")

        last_sql = st.session_state.get("last_sql")
        if last_sql:
            st.code(last_sql, language="sql")
            st.caption(
                "Copy-paste this into the Athena console to debug or iterate manually."
            )
        else:
            st.info(
                "No SQL captured yet. Ask a question that causes the agent to run a query "
                "and include a ```sql ... ``` block in its answer."
            )

    # ---------------- Chart section (if we have aggregated data) ----------------

    st.markdown("---")
    st.subheader("📊 Latest aggregated result (chart)")

    chart_df = st.session_state.get("last_chart_df")

    if chart_df is not None and not chart_df.empty:
        x_col = st.session_state.get("chart_x_col") or chart_df.columns[0]

        # Pick y_col: stored in state or first numeric column
        y_col = st.session_state.get("chart_y_col")
        if y_col is None or y_col not in chart_df.columns:
            numeric_cols = [
                c for c in chart_df.columns
                if pd.api.types.is_numeric_dtype(chart_df[c])
            ]
            y_col = numeric_cols[0] if numeric_cols else None

        if y_col is not None:
            st.caption(f"Plotting **{y_col}** by **{x_col}**")
            # Use index = x axis labels
            st.bar_chart(chart_df.set_index(x_col)[y_col])
        else:
            st.info(
                "I found a table, but couldn't identify a numeric column to plot. "
                "Showing the raw table instead."
            )

        st.dataframe(chart_df, use_container_width=True)
    else:
        st.info(
            "Ask a question that returns a small aggregated result (e.g. 'top 5', "
            "grouped counts). If the assistant includes a markdown table or a "
            "numbered list like '1. Name (123)', I'll plot it here."
        )

# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
# --------------------------------------------------------------------