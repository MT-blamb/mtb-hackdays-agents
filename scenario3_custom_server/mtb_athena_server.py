# scenario3_custom_server/mtb_athena_server.py
"""
MCP server exposing read-only Athena tools for Hackdays.

Tools:
  - list_tables(database=None)
  - describe_table(database, table)
  - run_readonly_query(database, sql, max_rows=50)
"""

from typing import List, Dict, Any
import time
import os
import boto3

from mcp.server.fastmcp import FastMCP   # <-- correct import (no InitializationOptions)

# ---- Configuration ----

ATHENA_WORKGROUP = os.getenv("MTB_ATHENA_WORKGROUP", "DataLakeWorkgroup-v3-production")
ATHENA_OUTPUT_LOCATION = os.getenv(
    "MTB_ATHENA_OUTPUT_LOCATION",
    "s3://jp-data-lake-athena-query-results-production/DataLakeWorkgroup-v3-production/",
)
DEFAULT_DATABASE = os.getenv(
    "MTB_ATHENA_DEFAULT_DB", 
    "lakehouse_experimental_jp_production"
)

# Read-only enforcement
FORBIDDEN_WORDS = ["insert", "update", "delete", "create", "drop", "alter", "truncate"]

# Create the MCP server
mcp = FastMCP("mtb_athena")
athena = boto3.client("athena")


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _wait_for_query(query_id: str, timeout_sec: int = 60) -> None:
    """Poll Athena until query is SUCCEEDED or FAILED/CANCELLED."""
    start = time.time()
    while True:
        resp = athena.get_query_execution(QueryExecutionId=query_id)
        state = resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return

        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason")
            raise RuntimeError(f"Athena query {state}: {reason}")

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Athena query timed out after {timeout_sec}s")

        time.sleep(1)


def _get_rows_raw(query_id: str):
    """Return rows (excluding header) and columns."""
    resp = athena.get_query_results(QueryExecutionId=query_id)
    rows = resp["ResultSet"]["Rows"]

    if not rows:
        return [], []

    header = rows[0]
    data_rows = rows[1:]

    columns = [c["VarCharValue"] for c in header["Data"]]
    data = [[c.get("VarCharValue") for c in r["Data"]] for r in data_rows]

    return data, columns


# ------------------------------------------------------------
# Tools
# ------------------------------------------------------------

@mcp.tool()
async def list_tables(database: str | None = None) -> List[str]:
    """List Athena tables in a database."""
    db = database or DEFAULT_DATABASE

    resp = athena.start_query_execution(
        QueryString=f"SHOW TABLES IN {db}",
        QueryExecutionContext={"Database": db},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]

    _wait_for_query(qid)
    rows, _ = _get_rows_raw(qid)
    return [r[0] for r in rows if r and r[0]]


@mcp.tool()
async def describe_table(database: str, table: str) -> List[Dict[str, Any]]:
    """Describe table columns."""
    resp = athena.start_query_execution(
        QueryString=f"DESCRIBE {table}",
        QueryExecutionContext={"Database": database},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]

    _wait_for_query(qid)
    rows, _ = _get_rows_raw(qid)
    result = []

    for r in rows:
        if not r or not r[0] or r[0].startswith("#"):
            continue
        name = r[0]
        dtype = r[1] if len(r) > 1 else ""
        comment = r[2] if len(r) > 2 else ""
        result.append({"name": name, "type": dtype, "comment": comment})

    return result


@mcp.tool()
async def run_readonly_query(database: str, sql: str, max_rows: int = 50):
    """Run a SELECT-only Athena query."""
    lowered = sql.lower()

    if any(w in lowered for w in FORBIDDEN_WORDS):
        raise ValueError("This tool only allows SELECT queries.")

    if not lowered.strip().startswith("select"):
        raise ValueError("Query must start with SELECT.")

    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]

    _wait_for_query(qid)
    rows, columns = _get_rows_raw(qid)
    rows = rows[:max_rows]

    return [dict(zip(columns, row)) for row in rows]


# ------------------------------------------------------------
# Start server (STDIO)
# ------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="stdio")
