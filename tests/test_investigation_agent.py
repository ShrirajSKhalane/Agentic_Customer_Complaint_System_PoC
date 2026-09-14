from __future__ import annotations

import os
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import InMemorySaver

from poc.db import connection, fetch_all
from poc.events import EventEnvelope
from poc.investigation_agent import (
    CONSUMER_NAME,
    InvestigationPlan,
    InvestigationRuntime,
    classified_event,
    create_app,
    duplicate_groups,
    normalize_lookup_tools,
    plan_investigation,
    plan_with_openai,
    should_interrupt,
    stub_investigation_plan,
    sync_assess_policy,
    sync_call_tool,
)
from poc.llm import OPENAI_MODEL
from tests.test_mcp_server import SEED_REFUND_TXNS, delete_refunds


CUST_1001_TXNS = [
    {
        "transactionId": "TXN-1001-A",
        "amount": 5000.0,
        "merchant": "Metro Electronics",
        "transactionTime": "2026-08-30T10:00:00Z",
    },
    {
        "transactionId": "TXN-1001-B",
        "amount": 5000.0,
        "merchant": "Metro Electronics",
        "transactionTime": "2026-08-30T10:02:00Z",
    },
]

# Two duplicate pairs, so "refund the duplicate" does not identify a charge.
CUST_1003_TXNS = [
    {
        "transactionId": "TXN-1003-A",
        "amount": 4500.0,
        "merchant": "Skyline Foods",
        "transactionTime": "2026-09-02T13:10:00Z",
    },
    {
        "transactionId": "TXN-1003-B",
        "amount": 4500.0,
        "merchant": "Skyline Foods",
        "transactionTime": "2026-09-02T13:12:00Z",
    },
    {
        "transactionId": "TXN-1003-C",
        "amount": 8900.0,
        "merchant": "Trailhead Outdoors",
        "transactionTime": "2026-09-06T18:40:00Z",
    },
    {
        "transactionId": "TXN-1003-D",
        "amount": 8900.0,
        "merchant": "Trailhead Outdoors",
        "transactionTime": "2026-09-06T18:43:00Z",
    },
]


class FakeTools:
    def __init__(self, transactions: list[dict[str, object]] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.transactions = transactions if transactions is not None else CUST_1001_TXNS

    def __call__(self, name: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append((name, arguments))
        if name == "get_transactions":
            return {"transactions": self.transactions}
        if name == "get_customer":
            return {"customer": {"customerId": arguments["customer_id"], "fraudFlag": False}}
        if name == "get_refund_status":
            return {
                "transactionId": arguments["transaction_id"],
                "refunded": False,
                "refunds": [],
            }
        raise AssertionError(f"unexpected tool {name}")


def fake_policy(**kwargs: object) -> dict[str, object]:
    return {
        "eligible": True,
        "action": "REFUND",
        "reason": "Duplicate payment qualifies for automatic refund.",
        "confidence": 0.94,
    }


def postgres_available() -> bool:
    try:
        with connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def insert_complaint(complaint_id: str, customer_id: str = "CUST-1001") -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO complaints (complaint_id, customer_id, message)
            VALUES (%s, %s, %s)
            ON CONFLICT (complaint_id) DO NOTHING
            """,
            (complaint_id, customer_id, "test complaint"),
        )
        conn.commit()


def make_runtime(
    tools: FakeTools | None = None,
    *,
    publisher: list[EventEnvelope] | None = None,
    policy: object = fake_policy,
    planner: object | None = None,
) -> tuple[InvestigationRuntime, FakeTools, list[EventEnvelope]]:
    tools = tools or FakeTools()
    published = publisher if publisher is not None else []
    runtime = InvestigationRuntime(
        checkpointer=InMemorySaver(),
        publisher=published.append,
        tool_caller=tools,
        policy_caller=policy,
        planner=planner
        or (lambda **kwargs: stub_investigation_plan(str(kwargs["customer_id"]))),
    )
    return runtime, tools, published


def test_duplicate_groups_detects_same_merchant_day_amount() -> None:
    groups = duplicate_groups(CUST_1001_TXNS, 5000)
    assert len(groups) == 1
    assert {item["transactionId"] for item in groups[0]} == {"TXN-1001-A", "TXN-1001-B"}


def test_one_duplicate_pair_needs_no_fact() -> None:
    assert should_interrupt({"transactions": CUST_1001_TXNS, "amount": 5000}) is False


def test_two_duplicate_pairs_need_a_fact() -> None:
    assert should_interrupt({"transactions": CUST_1003_TXNS, "amount": None}) is True
    assert should_interrupt({"transactions": CUST_1003_TXNS, "amount": 4500}) is False


def test_several_charges_without_a_duplicate_pair_need_a_fact() -> None:
    unrelated = [CUST_1003_TXNS[0], CUST_1003_TXNS[2]]
    assert should_interrupt({"transactions": unrelated, "amount": None}) is True


def test_answered_fact_does_not_interrupt_again() -> None:
    state = {
        "transactions": CUST_1003_TXNS,
        "amount": None,
        "fact_answer": "TXN-1003-B",
    }
    assert should_interrupt(state) is False


def test_plan_true_interrupts_only_when_more_than_one_candidate() -> None:
    assert should_interrupt(
        {"transactions": CUST_1003_TXNS, "amount": None, "ask_which_charge": True}
    ) is True
    assert should_interrupt(
        {"transactions": CUST_1001_TXNS, "amount": 5000, "ask_which_charge": True}
    ) is False


def test_plan_false_cannot_skip_interrupt_when_ledger_is_ambiguous() -> None:
    assert should_interrupt(
        {"transactions": CUST_1003_TXNS, "amount": None, "ask_which_charge": False}
    ) is True


def test_plan_false_does_not_interrupt_unambiguous_duplicate() -> None:
    assert should_interrupt(
        {"transactions": CUST_1001_TXNS, "amount": 5000, "ask_which_charge": False}
    ) is False


def test_stub_plans_match_seeded_customers() -> None:
    ananya = stub_investigation_plan("CUST-1001")
    vikram = stub_investigation_plan("CUST-1002")
    rohit = stub_investigation_plan("CUST-1003")
    assert ananya.ask_which_charge is False
    assert vikram.ask_which_charge is False
    assert rohit.ask_which_charge is True
    for plan in (ananya, vikram, rohit):
        assert plan.tools == ["get_transactions", "get_customer"]
    fallback = stub_investigation_plan("CUST-UNKNOWN")
    assert fallback.ask_which_charge is None


def test_normalize_lookup_tools_always_fetches_transactions() -> None:
    assert normalize_lookup_tools(["get_customer", "initiate_refund"]) == [
        "get_transactions",
        "get_customer",
    ]
    assert normalize_lookup_tools([]) == ["get_transactions"]


def test_stub_plan_investigation_does_not_need_openai() -> None:
    plan = plan_investigation(customer_id="CUST-1003", message="twice", stub=True)
    assert plan.ask_which_charge is True


def test_plan_investigation_without_key_does_not_silently_stub() -> None:
    previous = os.environ.pop("OPENAI_API_KEY", None)
    try:
        with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
            plan_investigation(
                customer_id="CUST-1003",
                message="I have been charged twice for the same payment.",
                stub=False,
            )
    finally:
        if previous is not None:
            os.environ["OPENAI_API_KEY"] = previous


def test_plan_investigation_stub_false_uses_injected_client() -> None:
    class FakeOpenAI:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.chat = SimpleNamespace(completions=SimpleNamespace(parse=self._parse))

        def _parse(self, **kwargs: object) -> SimpleNamespace:
            self.calls.append(kwargs)
            parsed = InvestigationPlan(
                tools=["get_transactions"],
                ask_which_charge=False,
                reason="Skip the fact question.",
            )
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(parsed=parsed))]
            )

    client = FakeOpenAI()
    plan = plan_investigation(
        customer_id="CUST-1003",
        message="I have been charged twice for the same payment.",
        stub=False,
        client=client,
    )
    assert client.calls
    assert plan.ask_which_charge is False


def test_analyze_does_not_pick_first_duplicate_pair_when_ambiguous() -> None:
    runtime, tools, _published = make_runtime()
    result = runtime.analyze_and_refund(
        {
            "complaint_id": "CMP-AMBIGUOUS",
            "transactions": CUST_1003_TXNS,
            "amount": None,
            "mcp_calls": [],
        }
    )
    assert result["finding"] == "DUPLICATE_TRANSACTION"
    assert result["transaction_id"] is None
    assert result["refund_status"] == "NOT_REFUNDED"
    assert tools.calls == []


def test_openai_structured_plan_uses_gpt_4o_mini() -> None:
    class FakeOpenAI:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.chat = SimpleNamespace(completions=SimpleNamespace(parse=self._parse))

        def _parse(self, **kwargs: object) -> SimpleNamespace:
            self.calls.append(kwargs)
            parsed = InvestigationPlan(
                tools=["get_customer"],
                ask_which_charge=True,
                reason="Ask which charge.",
            )
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(parsed=parsed))]
            )

    client = FakeOpenAI()
    plan = plan_with_openai(
        customer_id="CUST-1003",
        message="I have been charged twice for the same payment.",
        client=client,
    )
    assert client.calls
    assert client.calls[0]["model"] == OPENAI_MODEL
    assert client.calls[0]["response_format"] is InvestigationPlan
    assert plan.tools == ["get_transactions", "get_customer"]
    assert plan.ask_which_charge is True


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_cust_1001_gathers_mcp_tools_and_completes() -> None:
    runtime, tools, published = make_runtime()
    event = classified_event(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
    )
    insert_complaint(event.correlation_id)
    assert runtime.handle_complaint_classified(event) is True
    tool_names = [name for name, _ in tools.calls]
    assert "get_transactions" in tool_names
    assert tool_names.count("get_refund_status") >= 1
    types = [item.event_type for item in published]
    assert types == ["InvestigationStarted", "InvestigationCompleted"]
    completed = published[1]
    assert completed.payload["finding"] == "DUPLICATE_TRANSACTION"
    assert completed.payload["refundStatus"] == "NOT_REFUNDED"
    assert completed.payload["policyEligible"] is True
    assert completed.payload["confidence"] >= 0.90
    assert completed.correlation_id == event.correlation_id
    traces = fetch_all(
        """
        SELECT action FROM event_trace
        WHERE correlation_id = %s AND agent = %s
        ORDER BY created_at, id
        """,
        (event.correlation_id, CONSUMER_NAME),
    )
    actions = [row["action"] for row in traces]
    assert "InvestigationStarted" in actions
    assert "LLM:PLAN_INVESTIGATION" in actions
    assert "MCP:get_transactions" in actions
    assert "MCP:get_refund_status" in actions
    assert "A2A:ASSESS_POLICY" in actions
    assert "InvestigationCompleted" in actions


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_checkpoint_skips_second_classified_without_new_refund_lookup() -> None:
    tools = FakeTools()
    published: list[EventEnvelope] = []
    runtime, _, _ = make_runtime(tools, publisher=published)
    first = classified_event(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
    )
    insert_complaint(first.correlation_id)
    assert runtime.handle_complaint_classified(first) is True
    refund_calls = [call for call in tools.calls if call[0] == "get_refund_status"]
    second = EventEnvelope.create(
        "ComplaintClassified",
        first.correlation_id,
        dict(first.payload),
    )
    assert second.event_id != first.event_id
    assert runtime.handle_complaint_classified(second) is False
    assert [call for call in tools.calls if call[0] == "get_refund_status"] == refund_calls
    assert [item.event_type for item in published].count("InvestigationStarted") == 1
    assert [item.event_type for item in published].count("InvestigationCompleted") == 1


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_failed_plan_checkpoint_is_retried() -> None:
    attempts = {"n": 0}

    def flaky_planner(**kwargs: object) -> InvestigationPlan:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("plan failed")
        return stub_investigation_plan(str(kwargs["customer_id"]))

    runtime, _tools, published = make_runtime(planner=flaky_planner)
    event = classified_event(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
    )
    insert_complaint(event.correlation_id)
    with pytest.raises(RuntimeError, match="plan failed"):
        runtime.handle_complaint_classified(event)
    assert [item.event_type for item in published] == ["InvestigationStarted"]
    assert runtime.handle_complaint_classified(event) is True
    assert [item.event_type for item in published][-1] == "InvestigationCompleted"
    assert attempts["n"] == 2


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_fact_interrupt_holds_completed_until_resume() -> None:
    runtime, tools, published = make_runtime(FakeTools(CUST_1003_TXNS))
    event = classified_event(
        customer_id="CUST-1003",
        complaint_type="DUPLICATE_CHARGE",
        amount=None,
    )
    insert_complaint(event.correlation_id, "CUST-1003")
    assert runtime.handle_complaint_classified(event) is True
    assert [item.event_type for item in published] == ["InvestigationStarted"]
    pending = runtime.pending_prompt(event.correlation_id)
    assert pending is not None
    assert "charge" in pending["prompt"].lower()
    candidates = pending["detail"]["candidates"]
    assert {item["transactionId"] for item in candidates} == {"TXN-1003-B", "TXN-1003-D"}
    snapshot = runtime.graph.get_state({"configurable": {"thread_id": event.correlation_id}})
    assert snapshot.values.get("transactions")
    assert snapshot.values.get("ask_which_charge") is True
    assert "get_transactions" in [name for name, _ in tools.calls]
    traces = fetch_all(
        """
        SELECT action FROM event_trace
        WHERE correlation_id = %s AND agent = %s
        ORDER BY created_at, id
        """,
        (event.correlation_id, CONSUMER_NAME),
    )
    assert "LLM:PLAN_INVESTIGATION" in [row["action"] for row in traces]
    assert "get_refund_status" not in [name for name, _ in tools.calls]

    client = TestClient(create_app(runtime))
    shown = client.get(f"/pending/{event.correlation_id}")
    assert shown.status_code == 200
    resumed = client.post(
        f"/resume/{event.correlation_id}",
        json={"answer": "TXN-1003-B"},
    )
    assert resumed.status_code == 200
    assert [item.event_type for item in published] == [
        "InvestigationStarted",
        "InvestigationCompleted",
    ]
    completed = published[1]
    assert completed.payload["finding"] == "DUPLICATE_TRANSACTION"
    assert completed.payload["transactionId"] == "TXN-1003-B"
    assert completed.payload["amount"] == 4500
    assert snapshot_transactions_still_present(runtime, event.correlation_id)
    assert "get_refund_status" in [name for name, _ in tools.calls]


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_false_plan_still_interrupts_when_two_duplicate_pairs() -> None:
    runtime, tools, published = make_runtime(
        FakeTools(CUST_1003_TXNS),
        planner=lambda **_kwargs: InvestigationPlan(
            tools=["get_transactions", "get_customer"],
            ask_which_charge=False,
            reason="Live model skipped the fact question.",
        ),
    )
    event = classified_event(
        customer_id="CUST-1003",
        complaint_type="DUPLICATE_CHARGE",
        amount=None,
    )
    insert_complaint(event.correlation_id, "CUST-1003")
    assert runtime.handle_complaint_classified(event) is True
    assert [item.event_type for item in published] == ["InvestigationStarted"]
    pending = runtime.pending_prompt(event.correlation_id)
    assert pending is not None
    assert {item["transactionId"] for item in pending["detail"]["candidates"]} == {
        "TXN-1003-B",
        "TXN-1003-D",
    }
    assert "get_refund_status" not in [name for name, _ in tools.calls]


def snapshot_transactions_still_present(runtime: InvestigationRuntime, complaint_id: str) -> bool:
    snapshot = runtime.graph.get_state({"configurable": {"thread_id": complaint_id}})
    return bool(snapshot.values.get("transactions"))


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_unambiguous_duplicate_does_not_interrupt() -> None:
    runtime, _, published = make_runtime()
    event = classified_event(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
    )
    insert_complaint(event.correlation_id)
    runtime.handle_complaint_classified(event)
    assert runtime.pending_prompt(event.correlation_id) is None
    assert [item.event_type for item in published][-1] == "InvestigationCompleted"
    missing = uuid4().hex
    client = TestClient(create_app(runtime))
    assert client.get(f"/pending/{missing}").status_code == 404


@pytest.mark.skipif(not postgres_available(), reason="PostgreSQL is not available")
def test_gather_follows_planned_lookup_tools() -> None:
    runtime, tools, published = make_runtime(
        planner=lambda **_kwargs: InvestigationPlan(
            tools=["get_transactions"],
            ask_which_charge=False,
            reason="Transactions only.",
        )
    )
    event = classified_event(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
    )
    insert_complaint(event.correlation_id)
    runtime.handle_complaint_classified(event)
    assert [name for name, _ in tools.calls][0] == "get_transactions"
    assert "get_customer" not in [name for name, _ in tools.calls]
    assert "get_refund_status" in [name for name, _ in tools.calls]
    assert [item.event_type for item in published][-1] == "InvestigationCompleted"


def mcp_and_policy_available() -> bool:
    urls = (
        os.getenv("MCP_HEALTH_URL", "http://mcp-server:8003/health"),
        "http://localhost:8003/health",
        os.getenv("POLICY_AGENT_HEALTH_URL", "http://policy-agent:8001/health"),
        "http://localhost:8001/health",
    )
    ok = 0
    for url in urls:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                ok += 1
        except httpx.HTTPError:
            continue
    return ok >= 2


@pytest.mark.skipif(
    not postgres_available() or not mcp_and_policy_available(),
    reason="mcp-server or policy-agent is not running",
)
def test_live_cust_1001_uses_mcp_and_a2a() -> None:
    delete_refunds(*SEED_REFUND_TXNS)
    published: list[EventEnvelope] = []
    runtime = InvestigationRuntime(
        checkpointer=InMemorySaver(),
        publisher=published.append,
        tool_caller=sync_call_tool,
        policy_caller=sync_assess_policy,
        planner=lambda **kwargs: stub_investigation_plan(str(kwargs["customer_id"])),
    )
    event = classified_event(
        customer_id="CUST-1001",
        complaint_type="DUPLICATE_CHARGE",
        amount=5000,
    )
    insert_complaint(event.correlation_id)
    assert runtime.handle_complaint_classified(event) is True
    completed = [item for item in published if item.event_type == "InvestigationCompleted"]
    assert len(completed) == 1
    payload = completed[0].payload
    assert payload["finding"] == "DUPLICATE_TRANSACTION"
    assert payload["refundStatus"] == "NOT_REFUNDED"
    assert payload["policyEligible"] is True
    assert payload["confidence"] >= 0.90
    traces = fetch_all(
        """
        SELECT action FROM event_trace
        WHERE correlation_id = %s AND agent = %s
        """,
        (event.correlation_id, CONSUMER_NAME),
    )
    actions = [row["action"] for row in traces]
    assert "LLM:PLAN_INVESTIGATION" in actions
    assert "MCP:get_transactions" in actions
    assert "MCP:get_refund_status" in actions
    assert "A2A:ASSESS_POLICY" in actions
