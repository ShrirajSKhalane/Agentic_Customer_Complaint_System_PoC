from __future__ import annotations

import asyncio
import os

import httpx
import pytest

from poc.db import connection
from poc.mcp_client import call_tool, list_tool_names
from poc.mcp_server import (
    TOOL_NAMES,
    initiate_refund_record,
    list_transaction_records,
    refund_status_record,
)

MCP_HEALTH_URLS = (
    os.getenv("MCP_HEALTH_URL", "http://mcp-server:8003/health"),
    "http://localhost:8003/health",
)


SEED_REFUND_TXNS = (
    "TXN-1001-A",
    "TXN-1001-B",
    "TXN-1002-A",
    "TXN-1003-A",
    "TXN-1003-B",
    "TXN-1003-C",
    "TXN-1003-D",
)


def delete_refunds(*transaction_ids: str) -> None:
    with connection() as conn:
        conn.execute(
            "DELETE FROM refunds WHERE transaction_id = ANY(%s)",
            (list(transaction_ids),),
        )
        conn.commit()


def mcp_server_available() -> bool:
    for url in MCP_HEALTH_URLS:
        try:
            response = httpx.get(url, timeout=1)
            if response.status_code == 200:
                return True
        except httpx.HTTPError:
            continue
    return False


def test_seeded_transactions_match_demo_facts() -> None:
    delete_refunds(*SEED_REFUND_TXNS)
    cust_1001 = list_transaction_records("CUST-1001")
    amounts_1001 = sorted(item["amount"] for item in cust_1001)
    assert amounts_1001 == [5000.0, 5000.0]
    assert len({item["transactionId"] for item in cust_1001}) == 2
    for item in cust_1001:
        status = refund_status_record(item["transactionId"])
        assert status["refunded"] is False

    cust_1002 = list_transaction_records("CUST-1002")
    amounts_1002 = [item["amount"] for item in cust_1002]
    assert 50000.0 in amounts_1002
    assert amounts_1002.count(50000.0) == 1

    cust_1003 = list_transaction_records("CUST-1003")
    amounts_1003 = sorted(item["amount"] for item in cust_1003)
    assert amounts_1003 == [4500.0, 4500.0, 8900.0, 8900.0]


def test_initiate_refund_is_unique_on_transaction_and_amount() -> None:
    try:
        first = initiate_refund_record("TXN-1002-A", 1.23)
        second = initiate_refund_record("TXN-1002-A", 1.23)
        assert second["created"] is False
        assert first["refund"]["refundId"] == second["refund"]["refundId"]
        status = refund_status_record("TXN-1002-A")
        matching = [
            refund
            for refund in status["refunds"]
            if refund["amount"] == 1.23
        ]
        assert len(matching) == 1
    finally:
        delete_refunds("TXN-1002-A")


@pytest.mark.skipif(not mcp_server_available(), reason="mcp-server is not running")
def test_mcp_list_tools_and_seeded_lookups() -> None:
    names = asyncio.run(list_tool_names())
    assert names == sorted(TOOL_NAMES)

    cust_1001 = asyncio.run(
        call_tool("get_transactions", {"customer_id": "CUST-1001"})
    )["transactions"]
    assert sorted(item["amount"] for item in cust_1001) == [5000.0, 5000.0]

    cust_1002 = asyncio.run(
        call_tool("get_transactions", {"customer_id": "CUST-1002"})
    )["transactions"]
    assert [item["amount"] for item in cust_1002].count(50000.0) == 1
    assert [item["amount"] for item in cust_1002].count(5000.0) == 0

    first = asyncio.run(
        call_tool(
            "initiate_refund",
            {"transaction_id": "TXN-1002-A", "amount": 1.23},
        )
    )
    second = asyncio.run(
        call_tool(
            "initiate_refund",
            {"transaction_id": "TXN-1002-A", "amount": 1.23},
        )
    )
    try:
        assert first["refund"]["refundId"] == second["refund"]["refundId"]
        assert second["created"] is False
        status = asyncio.run(
            call_tool("get_refund_status", {"transaction_id": "TXN-1002-A"})
        )
        assert len(status["refunds"]) == 1
    finally:
        delete_refunds("TXN-1002-A")
