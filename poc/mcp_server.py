from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from mcp.server.mcpserver import MCPServer
from starlette.requests import Request
from starlette.responses import JSONResponse

from poc.db import connection, fetch_all, fetch_one


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = MCPServer(
    name="complaint-business-tools",
    instructions=(
        "Mock banking and policy tools for the complaint-resolution PoC. "
        "Use these tools for customer, account, transaction, refund, and policy data."
    ),
)

TOOL_NAMES = (
    "get_customer",
    "get_account",
    "get_transaction",
    "get_transactions",
    "get_refund_status",
    "initiate_refund",
    "get_policy",
)


def jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {camel_key(str(key)): jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def camel_key(key: str) -> str:
    parts = key.split("_")
    return parts[0] + "".join(part.title() for part in parts[1:])


def require_row(row: dict[str, Any] | None, message: str) -> dict[str, Any]:
    if row is None:
        raise ValueError(message)
    return jsonable(row)


def parse_amount(amount: float | int | str | Decimal) -> Decimal:
    return Decimal(str(amount)).quantize(Decimal("0.01"))


def get_customer_record(customer_id: str) -> dict[str, Any]:
    return require_row(
        fetch_one(
            "SELECT * FROM customers WHERE customer_id = %s",
            (customer_id,),
        ),
        f"Unknown customer {customer_id}",
    )


def get_account_record(customer_id: str) -> dict[str, Any]:
    return require_row(
        fetch_one(
            """
            SELECT * FROM accounts
            WHERE customer_id = %s
            ORDER BY account_id
            LIMIT 1
            """,
            (customer_id,),
        ),
        f"Unknown account for customer {customer_id}",
    )


def get_transaction_record(transaction_id: str) -> dict[str, Any]:
    return require_row(
        fetch_one(
            "SELECT * FROM transactions WHERE transaction_id = %s",
            (transaction_id,),
        ),
        f"Unknown transaction {transaction_id}",
    )


def list_transaction_records(
    customer_id: str, date_range: str | None = None
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM transactions WHERE customer_id = %s"
    params: list[Any] = [customer_id]
    if date_range:
        parts = [part.strip() for part in date_range.replace("/", ",").split(",") if part.strip()]
        if len(parts) == 2:
            sql += " AND transaction_time >= %s AND transaction_time <= %s"
            params.extend(parts)
    sql += " ORDER BY transaction_time, transaction_id"
    return jsonable(fetch_all(sql, tuple(params)))


def refund_status_record(transaction_id: str) -> dict[str, Any]:
    get_transaction_record(transaction_id)
    refunds = jsonable(
        fetch_all(
            """
            SELECT * FROM refunds
            WHERE transaction_id = %s
            ORDER BY created_at, refund_id
            """,
            (transaction_id,),
        )
    )
    return {
        "transactionId": transaction_id,
        "refunded": bool(refunds),
        "refunds": refunds,
    }


def initiate_refund_record(
    transaction_id: str, amount: float | int | str | Decimal
) -> dict[str, Any]:
    get_transaction_record(transaction_id)
    refund_amount = parse_amount(amount)
    with connection() as conn:
        inserted = conn.execute(
            """
            INSERT INTO refunds (refund_id, transaction_id, amount)
            VALUES (%s, %s, %s)
            ON CONFLICT (transaction_id, amount) DO NOTHING
            RETURNING *
            """,
            (str(uuid4()), transaction_id, refund_amount),
        ).fetchone()
        created = inserted is not None
        row = inserted or conn.execute(
            """
            SELECT * FROM refunds
            WHERE transaction_id = %s AND amount = %s
            """,
            (transaction_id, refund_amount),
        ).fetchone()
        conn.commit()
    if row is None:
        raise RuntimeError("Refund could not be recorded")
    return {"created": created, "refund": jsonable(row)}


def get_policy_record(policy_type: str) -> dict[str, Any]:
    return require_row(
        fetch_one(
            """
            SELECT * FROM policies
            WHERE policy_key = %s OR complaint_type = %s
            """,
            (policy_type, policy_type),
        ),
        f"Unknown policy {policy_type}",
    )


@mcp.custom_route("/health", methods=["GET"])
async def health(_request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


@mcp.tool()
def get_customer(customer_id: str) -> dict[str, Any]:
    """Return a customer profile by customer id."""
    return {"customer": get_customer_record(customer_id)}


@mcp.tool()
def get_account(customer_id: str) -> dict[str, Any]:
    """Return the primary account for a customer."""
    return {"account": get_account_record(customer_id)}


@mcp.tool()
def get_transaction(transaction_id: str) -> dict[str, Any]:
    """Return a single transaction by id."""
    return {"transaction": get_transaction_record(transaction_id)}


@mcp.tool()
def get_transactions(
    customer_id: str, date_range: str | None = None
) -> dict[str, Any]:
    """Return transactions for a customer, optionally filtered by date_range."""
    return {"transactions": list_transaction_records(customer_id, date_range)}


@mcp.tool()
def get_refund_status(transaction_id: str) -> dict[str, Any]:
    """Return whether a transaction has been refunded."""
    return refund_status_record(transaction_id)


@mcp.tool()
def initiate_refund(transaction_id: str, amount: float) -> dict[str, Any]:
    """Record a refund for a transaction and amount. Identical repeats return the existing refund."""
    return initiate_refund_record(transaction_id, amount)


@mcp.tool()
def get_policy(policy_type: str) -> dict[str, Any]:
    """Return the policy text used to assess a complaint type."""
    return {"policy": get_policy_record(policy_type)}


async def run() -> None:
    logger.info("Starting MCP server on 0.0.0.0:8003/mcp")
    await mcp.run_streamable_http_async(
        host="0.0.0.0",
        port=8003,
        streamable_http_path="/mcp",
        stateless_http=True,
    )


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
