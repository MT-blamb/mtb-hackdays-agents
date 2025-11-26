# scenario3_custom_server/mtb_athena_server.py
"""
MCP server exposing read-only Athena tools for Hackdays.

Tools:
  - list_tables(database=None)
  - describe_table(database, table)
  - run_readonly_query(database, sql, max_rows=50)
"""

from typing import List, Dict, Any
import os
import time

import boto3
from mcp.server.fastmcp import FastMCP

# ---- Configuration ----

ATHENA_WORKGROUP = os.getenv(
    "MTB_ATHENA_WORKGROUP",
    "DataLakeWorkgroup-v3-production",
)
ATHENA_OUTPUT_LOCATION = os.getenv(
    "MTB_ATHENA_OUTPUT_LOCATION",
    "s3://jp-data-lake-athena-query-results-production/"
    "DataLakeWorkgroup-v3-production/",
)
DEFAULT_DATABASE = os.getenv(
    "MTB_ATHENA_DEFAULT_DB",
    "lakehouse_experimental_jp_production",
)

# Configurable timeout for Athena queries (seconds)
DEFAULT_QUERY_TIMEOUT_SEC = int(
    os.getenv("MTB_ATHENA_QUERY_TIMEOUT_SEC", "180")
)

# Words we do NOT allow in queries (safety)
FORBIDDEN_WORDS = [
    "insert",
    "update",
    "delete",
    "create",
    "drop",
    "alter",
    "truncate",
]

mcp = FastMCP("mtb_athena")
athena = boto3.client("athena")


# ---- Athena helpers ----


def _wait_for_query(query_id: str, timeout_sec: int | None = None) -> None:
    """Poll Athena until query is SUCCEEDED or FAILED/CANCELLED."""
    timeout = timeout_sec or DEFAULT_QUERY_TIMEOUT_SEC
    start = time.time()

    while True:
        resp = athena.get_query_execution(QueryExecutionId=query_id)
        status = resp["QueryExecution"]["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            return

        if state in ("FAILED", "CANCELLED"):
            reason = status.get("StateChangeReason", "Unknown")
            raise RuntimeError(
                f"Athena query {state}. "
                f"QueryExecutionId={query_id}. "
                f"Reason={reason}"
            )

        if time.time() - start > timeout:
            raise TimeoutError(
                f"Athena query timed out after {timeout}s "
                f"(QueryExecutionId={query_id})"
            )

        time.sleep(1)


def _get_rows_raw(query_id: str):
    """Return rows (excluding header) and column names."""
    resp = athena.get_query_results(QueryExecutionId=query_id)
    rows = resp["ResultSet"]["Rows"]

    if not rows:
        return [], []

    header_row = rows[0]
    data_rows = rows[1:]

    columns = [c["VarCharValue"] for c in header_row["Data"]]
    data = [[c.get("VarCharValue") for c in r["Data"]] for r in data_rows]
    return data, columns


# ---- MCP Tools ----


@mcp.tool()
async def list_tables(database: str | None = None) -> List[str]:
    """
    List Athena tables for a given database.

    Args:
        database: Athena database name. If omitted, uses MTB_ATHENA_DEFAULT_DB.
    """
    db = database or DEFAULT_DATABASE
    if not db:
        raise ValueError(
            "No database provided and MTB_ATHENA_DEFAULT_DB is not set."
        )

    resp = athena.start_query_execution(
        QueryString=f"SHOW TABLES IN {db}",
        QueryExecutionContext={"Database": db},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]
    _wait_for_query(qid)
    rows, _ = _get_rows_raw(qid)

    tables = [r[0] for r in rows if r and r[0]]
    return tables


@mcp.tool()
async def describe_table(database: str, table: str) -> List[Dict[str, Any]]:
    """
    Describe columns for a table: name, type, comment.

    Args:
        database: Athena database name
        table: table name
    """
    resp = athena.start_query_execution(
        QueryString=f"DESCRIBE {table}",
        QueryExecutionContext={"Database": database},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]
    _wait_for_query(qid)
    rows, _ = _get_rows_raw(qid)

    result: List[Dict[str, Any]] = []
    for r in rows:
        if not r or not r[0] or r[0].startswith("#"):
            continue
        name = r[0]
        dtype = r[1] if len(r) > 1 else ""
        comment = r[2] if len(r) > 2 else ""
        result.append({"name": name, "type": dtype, "comment": comment})

    return result


@mcp.tool()
async def run_readonly_query(
    database: str,
    sql: str,
    max_rows: int = 50,
) -> List[Dict[str, Any]]:
    """
    Run a SELECT-only Athena query and return rows as list-of-dicts.

    Args:
        database: Athena database name
        sql: SQL query (must be read-only)
        max_rows: max number of rows to return (default 50)
    """
    lowered = sql.lower()
    if any(word in lowered for word in FORBIDDEN_WORDS):
        raise ValueError(
            "Only read-only queries (SELECT/SHOW/DESCRIBE) are allowed. "
            "Found one of: " + ", ".join(FORBIDDEN_WORDS)
        )

    if not lowered.strip().startswith("select"):
        raise ValueError("Queries must start with SELECT for this demo.")

    # Log SQL for debugging
    print(
        f"[mtb_athena] Running query on {database} "
        f"(max_rows={max_rows}):\n{sql}\n"
    )

    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
    )
    qid = resp["QueryExecutionId"]

    # Wait with configurable timeout
    _wait_for_query(qid)

    rows, columns = _get_rows_raw(qid)

    rows = rows[:max_rows]
    return [dict(zip(columns, row)) for row in rows]


# ---- Entry point ----

if __name__ == "__main__":
    # STDIO transport so the MCP client (agent) can talk to this process.
    mcp.run(transport="stdio")
