from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client


MCP_URL = os.getenv("MCP_URL", "http://localhost:8003/mcp")


@asynccontextmanager
async def mcp_session(url: str = MCP_URL) -> AsyncIterator[ClientSession]:
    async with streamable_http_client(url) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            yield session


def tool_payload(result: Any) -> dict[str, Any]:
    if getattr(result, "is_error", False):
        raise AssertionError(f"MCP tool error: {result}")
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, dict):
        return structured
    content = getattr(result, "content", None) or []
    texts = [item.text for item in content if getattr(item, "text", None)]
    raise AssertionError(f"Unexpected MCP tool result: {texts or result}")


async def list_tool_names(url: str = MCP_URL) -> list[str]:
    async with mcp_session(url) as session:
        listed = await session.list_tools()
        return sorted(tool.name for tool in listed.tools)


async def call_tool(
    name: str, arguments: dict[str, Any] | None = None, url: str = MCP_URL
) -> dict[str, Any]:
    async with mcp_session(url) as session:
        return tool_payload(await session.call_tool(name, arguments or {}))
