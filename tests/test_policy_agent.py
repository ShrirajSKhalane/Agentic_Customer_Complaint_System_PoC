from __future__ import annotations

import json
import os
from types import SimpleNamespace

import httpx
import pytest

from poc.a2a_client import assess_policy as a2a_assess_policy
from poc.llm import OPENAI_MODEL
from poc.policy_agent import (
    DEFAULT_QUESTION,
    PolicyAssessment,
    assess_policy,
    assess_with_openai,
    parse_assess_request,
    stub_assessment,
)


class FakeOpenAI:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(parse=self._parse))

    def _parse(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(kwargs)
        parsed = stub_assessment("CUST-1001", "DUPLICATE_CHARGE")
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(parsed=parsed))]
        )


async def fake_policy_loader(policy_type: str) -> dict[str, object]:
    return {
        "policyKey": policy_type,
        "complaintType": policy_type,
        "text": "Duplicate payment qualifies for automatic refund at or below INR 10,000.",
        "autoRefundLimit": 10000,
    }


def policy_agent_available() -> bool:
    urls = (
        os.getenv("POLICY_AGENT_HEALTH_URL", "http://policy-agent:8001/health"),
        "http://localhost:8001/health",
    )
    for url in urls:
        try:
            response = httpx.get(url, timeout=1)
            if response.status_code == 200:
                return True
        except httpx.HTTPError:
            continue
    return False


def policy_agent_base() -> str:
    for url in (
        os.getenv("POLICY_AGENT_URL", "http://policy-agent:8001"),
        "http://localhost:8001",
    ):
        try:
            response = httpx.get(f"{url.rstrip('/')}/health", timeout=1)
            if response.status_code == 200:
                return url.rstrip("/")
        except httpx.HTTPError:
            continue
    return "http://localhost:8001"


def test_parse_assess_policy_request() -> None:
    parsed = parse_assess_request(
        json.dumps(
            {
                "task": "ASSESS_POLICY",
                "context": {"customerId": "CUST-1001"},
                "question": DEFAULT_QUESTION,
            }
        )
    )
    assert parsed["task"] == "ASSESS_POLICY"
    assert parsed["context"]["customerId"] == "CUST-1001"


def test_stub_duplicate_charge_is_eligible_refund() -> None:
    result = stub_assessment("CUST-1001", "DUPLICATE_CHARGE")
    assert result is not None
    payload = result.as_payload()
    assert payload["eligible"] is True
    assert payload["action"] == "REFUND"
    assert "duplicate" in payload["reason"].lower()
    assert payload["confidence"] >= 0.90


@pytest.mark.asyncio
async def test_stub_mode_does_not_call_openai() -> None:
    client = FakeOpenAI()
    loaded: list[str] = []

    async def recording_loader(policy_type: str) -> dict[str, object]:
        loaded.append(policy_type)
        return await fake_policy_loader(policy_type)

    result = await assess_policy(
        "CUST-1001",
        "DUPLICATE_CHARGE",
        5000,
        stub=True,
        client=client,
        policy_loader=recording_loader,
    )
    unrecognized = await assess_policy(
        "CUST-1002",
        "UNRECOGNIZED_TRANSACTION",
        50000,
        stub=True,
        client=client,
        policy_loader=recording_loader,
    )
    assert client.calls == []
    assert loaded == ["DUPLICATE_CHARGE", "UNRECOGNIZED_TRANSACTION"]
    assert result.as_payload()["eligible"] is True
    assert unrecognized.as_payload()["eligible"] is False
    assert unrecognized.as_payload()["action"] == "HUMAN_REVIEW"


def test_openai_structured_assessment_uses_gpt_4o_mini() -> None:
    client = FakeOpenAI()
    result = assess_with_openai(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
        question=DEFAULT_QUESTION,
        policy={"text": "duplicate charges may be refunded"},
        client=client,
    )
    assert client.calls
    assert client.calls[0]["model"] == OPENAI_MODEL
    assert client.calls[0]["response_format"] is PolicyAssessment
    payload = result.as_payload()
    assert payload["eligible"] is True
    assert payload["action"] == "REFUND"


@pytest.mark.asyncio
@pytest.mark.skipif(not policy_agent_available(), reason="policy-agent is not running")
async def test_a2a_client_assesses_duplicate_charge_via_mcp() -> None:
    payload = await a2a_assess_policy(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
        complaint_id="CMP-POLICY-TEST",
        base_url=policy_agent_base(),
    )
    assert payload["eligible"] is True
    assert payload["action"] == "REFUND"
    assert "reason" in payload
    assert payload["confidence"] >= 0.90
