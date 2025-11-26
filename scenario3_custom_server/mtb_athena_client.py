# scenario3_custom_server/mtb_athena_client.py

import asyncio
import json
import sys
import os
from typing import Any, Dict, List

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import (
    CallToolResult,
    TextContent,
)

DEFAULT_DB = "lakehouse_experimental_jp_production"
DEMO_TABLE = "transactions"
DEMO_SQL = f"SELECT * FROM {DEMO_TABLE} LIMIT 5"


def _unwrap_call_tool_result(result: CallToolResult) -> Dict[str, Any]:
    as_dict = {
        "meta": result.meta,
        "content": result.content,
        "structuredContent": result.structuredContent,
        "isError": result.isError,
    }

    if result.isError:
        text_parts = [
            c.text
            for c in (result.content or [])
            if isinstance(c, TextContent)
        ]
        msg = text_parts[0] if text_parts else "Unknown MCP tool error"
        raise RuntimeError(f"Tool error: {msg}")

    if result.structuredContent is None:
        text_parts = [
            c.text
            for c in (result.content or [])
            if isinstance(c, TextContent)
        ]
        return {"text": "\n".join(text_parts)}

    return result.structuredContent


async def main() -> None:
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[
            "scenario3_custom_server/mtb_athena_server.py",
        ],
        env=dict(os.environ),
    )

    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:

            # 🚫 DO NOT CALL `await session.initialize()` — FastMCP handles it.

            tools_response = await session.list_tools()
            print("✅ Connected to mtb_athena MCP server.")
            print("Available tools:")
            for t in tools_response.tools:
                print(f"  - {t.name}: {t.description.strip() if t.description else ''}")

            print(f"\n📋 Listing tables in database: {DEFAULT_DB}")
            tables_result: CallToolResult = await session.call_tool(
                "list_tables",
                arguments={"database": DEFAULT_DB},
            )
            tables_payload = _unwrap_call_tool_result(tables_result)
            tables: List[str] = tables_payload.get("result", [])
            print(f"Found {len(tables)} tables. First 10:")
            for tname in tables[:10]:
                print(f"  - {tname}")

            if not tables:
                print("\n⚠️ No tables returned; double-check DEFAULT_DB.")
                return

            print(f"\n🔍 Running demo SELECT on {DEMO_TABLE} (LIMIT 5)...")
            rows_result: CallToolResult = await session.call_tool(
                "run_readonly_query",
                arguments={
                    "database": DEFAULT_DB,
                    "sql": DEMO_SQL,
                    "max_rows": 5,
                },
            )
            rows_payload = _unwrap_call_tool_result(rows_result)
            rows: List[Dict[str, Any]] = rows_payload.get("result", [])

            if not rows:
                print("Got 0 rows.")
            else:
                print(f"Got {len(rows)} row(s):\n")
                print(json.dumps(rows, indent=2, ensure_ascii=False))

            print("\n✅ Done. MCP + Athena is working 🎉")


if __name__ == "__main__":
    asyncio.run(main())
