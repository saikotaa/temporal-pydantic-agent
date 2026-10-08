"""ConversationWorkflow against the compose Temporal server with Pydantic AI test models."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.models.test import TestModel

from conftest import Harness
from toy.commands import Command
from toy.state import ConversationStatus, TurnTerminal
from toy.workflow import ConversationWorkflow, WorkflowInput


def cmd(conv: str, kind: str, cmid: str, **payload: Any) -> Command:
    return Command.model_validate(
        {"conversation_id": conv, "client_message_id": cmid, "payload": {"kind": kind, **payload}}
    )


class GatedModel:
    """FunctionModel whose stream waits for `release()`; records the messages it was given.

    With `tool_first=True` the first request of every turn calls `lookup_weather`, so the turn
    has a second model request (where queued steer is injected).
    """

    def __init__(self, *, tool_first: bool = False) -> None:
        self.gate = asyncio.Event()
        self.seen: list[list[ModelMessage]] = []
        self.tool_first = tool_first

    def release(self) -> None:
        self.gate.set()

    def model(self) -> FunctionModel:
        async def stream(
            messages: list[ModelMessage], info: AgentInfo
        ) -> AsyncIterator[str | DeltaToolCalls]:
            self.seen.append(messages)
            await self.gate.wait()
            last = messages[-1]
            is_first = isinstance(last, ModelRequest) and any(
                isinstance(p, UserPromptPart) for p in last.parts
            )
            if self.tool_first and is_first:
                yield {0: DeltaToolCall(name="lookup_weather", json_args='{"city": "Oslo"}')}
                return
            yield "gated "
            yield "reply"

        return FunctionModel(stream_function=stream, model_name="gated")


async def test_send_runs_turn_and_persists(harness: Harness) -> None:
    harness.set_model(TestModel(call_tools=[], custom_output_text="hello back"))
    ack = await harness.workflows.submit(cmd(harness.conversation_id, "send", "m1", content="hi"))
    assert (
        ack.accepted and not ack.duplicate and ack.workflow_id == f"conv:{harness.conversation_id}"
    )
    dup = await harness.workflows.submit(cmd(harness.conversation_id, "send", "m1", content="hi"))
    assert dup.duplicate

    state = await harness.wait_for(lambda s: s.turn_count == 1 and s.status == "idle")
    assert state.last_terminal == TurnTerminal.COMPLETED and state.run_number == 1
    assert len(harness.repo.turns) == 1
    turn = harness.repo.turns[0]
    assert turn.turn_id == "turn-1" and turn.terminal == TurnTerminal.COMPLETED
    assert isinstance(turn.messages[0], ModelRequest) and isinstance(
        turn.messages[1], ModelResponse
    )
    assert turn.usage and turn.usage["requests"] == 1
    await harness.wait_until(
        lambda: harness.event_types()[-2:] == ["turn.completed", "status.changed"]
    )
    types = harness.event_types()
    assert types[0] == "status.changed"
    assert "turn.started" in types and "text.delta" in types
    completed = harness.events_of("turn.completed")[0]
    assert completed.terminal == TurnTerminal.COMPLETED and completed.usage["requests"] == 1
    assert "".join(e.delta for e in harness.events_of("text.delta")) == "hello back"
    assert harness.repo.statuses[harness.conversation_id].status == ConversationStatus.IDLE


async def test_queue_edits_and_steer_while_working(harness: Harness) -> None:
    gated = GatedModel(tool_first=True)
    harness.set_model(gated.model())
    conv = harness.conversation_id
    await harness.workflows.submit(cmd(conv, "send", "m1", content="first"))
    await harness.wait_for(lambda s: s.status == "working")
    for i in (2, 3, 4):
        await harness.workflows.submit(cmd(conv, "send", f"m{i}", content=f"msg {i}"))
    steer = await harness.workflows.submit(cmd(conv, "steer", "s1", content="answer in French"))
    assert steer.accepted and steer.detail == "inbox"

    await harness.workflows.submit(
        cmd(conv, "edit", "e1", target_client_message_id="m3", content="edited 3")
    )
    await harness.workflows.submit(cmd(conv, "reorder", "r1", order=["m4", "m2"]))
    await harness.workflows.submit(cmd(conv, "delete", "d1", target_client_message_id="m3"))
    state = await harness.wait_for(
        lambda s: [m.client_message_id for m in s.pending] == ["m4", "m2"]
    )
    await harness.workflows.submit(cmd(conv, "send_now", "n1", target_client_message_id="m2"))
    state = await harness.wait_for(
        lambda s: [m.client_message_id for m in s.pending] == ["m2", "m4"]
    )
    assert state.current_client_message_id == "m1" and state.current_turn_id == "turn-1"

    gated.release()
    state = await harness.wait_for(lambda s: s.turn_count == 3 and s.status == "idle")
    assert state.pending == []
    # The steer was injected into the first turn's *second* model request (after the tool call).
    first_request = gated.seen[0][-1]
    assert isinstance(first_request, ModelRequest)
    assert [p.content for p in first_request.parts if isinstance(p, UserPromptPart)] == ["first"]
    second_request = gated.seen[1][-1]
    assert isinstance(second_request, ModelRequest)
    contents = [p.content for p in second_request.parts if isinstance(p, UserPromptPart)]
    assert contents == ["[Steering instruction from the user]: answer in French"]
    assert any(isinstance(p, ToolReturnPart) for p in second_request.parts)
    assert "tool.output" in harness.event_types()
    assert [t.turn_id for t in harness.repo.turns] == ["turn-1", "turn-2", "turn-3"]
    # Later turns must not see the steer again.
    later = [
        p.content for m in gated.seen[2:] for p in m[-1].parts if isinstance(p, UserPromptPart)
    ]
    assert all("Steering" not in str(c) for c in later)


async def test_stop_mid_turn(harness: Harness) -> None:
    gated = GatedModel()
    harness.set_model(gated.model())
    conv = harness.conversation_id
    await harness.workflows.submit(cmd(conv, "send", "m1", content="first"))
    await harness.wait_for(lambda s: s.status == "working" and s.current_turn_id == "turn-1")
    # Scoped stop for a different turn is ignored.
    await harness.workflows.submit(cmd(conv, "stop", "x1", expected_turn_id="turn-9"))
    await asyncio.sleep(0.5)
    assert (await harness.state()).status == ConversationStatus.WORKING
    ack = await harness.workflows.submit(cmd(conv, "stop", "x2", expected_turn_id="turn-1"))
    assert ack.accepted
    state = await harness.wait_for(lambda s: s.status == "idle")
    assert state.last_terminal == TurnTerminal.STOPPED and state.turn_count == 1
    assert harness.repo.turns == []
    await harness.wait_until(lambda: bool(harness.events_of("turn.completed")))
    assert harness.events_of("turn.completed")[0].terminal == TurnTerminal.STOPPED
    # The conversation still works afterwards.
    gated.release()
    await harness.workflows.submit(cmd(conv, "send", "m2", content="second"))
    state = await harness.wait_for(lambda s: s.turn_count == 2 and s.status == "idle")
    assert state.last_terminal == TurnTerminal.COMPLETED


@pytest.mark.parametrize("decision", ["accept", "decline"])
async def test_approval_flow(harness: Harness, decision: str) -> None:
    harness.set_model(TestModel(call_tools=["create_ticket"], custom_output_text="done"))
    conv = harness.conversation_id
    await harness.workflows.submit(cmd(conv, "send", "m1", content="open a ticket"))
    state = await harness.wait_for(lambda s: s.status == "blocked")
    assert state.last_terminal == TurnTerminal.BLOCKED and len(state.pending_approvals) == 1
    approval = state.pending_approvals[0]
    assert approval.tool_name == "create_ticket" and set(approval.args) == {"title", "body"}
    assert harness.repo.turns[0].terminal == TurnTerminal.BLOCKED
    await harness.wait_until(lambda: bool(harness.events_of("turn.completed")))
    blocked_event = harness.events_of("turn.completed")[0]
    assert blocked_event.terminal == TurnTerminal.BLOCKED and blocked_event.approvals

    # A plain send while blocked queues but does not run.
    await harness.workflows.submit(cmd(conv, "send", "m2", content="also this"))
    await asyncio.sleep(0.5)
    assert (await harness.state()).status == ConversationStatus.BLOCKED

    results = [{"tool_call_id": approval.tool_call_id, "decision": decision, "reason": "nope"}]
    ack = await harness.workflows.submit(cmd(conv, "tool_results", "t1", results=results))
    assert ack.accepted
    state = await harness.wait_for(lambda s: s.turn_count == 3 and s.status == "idle")
    assert state.pending_approvals == [] and state.last_terminal == TurnTerminal.COMPLETED
    await harness.wait_until(lambda: harness.event_types()[-1] == "status.changed")
    resolved = harness.events_of("request.resolved")[0]
    assert resolved.approvals[0]["approved"] is (decision == "accept")
    outputs = harness.events_of("tool.output")
    assert outputs and outputs[0].tool_name == "create_ticket"
    if decision == "accept":
        assert "created" in str(outputs[0].output)
    else:
        assert "nope" in str(outputs[0].output)
    seq = [t for t in harness.event_types() if t.startswith(("turn.", "request."))]
    assert seq[:4] == ["turn.started", "turn.completed", "turn.started", "request.resolved"]


async def test_continue_as_new_keeps_queue(harness: Harness) -> None:
    gated = GatedModel()
    harness.set_model(gated.model())
    conv = harness.conversation_id
    handle = await harness.client.start_workflow(
        ConversationWorkflow.run,
        WorkflowInput(conversation_id=conv, turns_per_run=2),
        id=f"conv:{conv}",
        task_queue=harness.task_queue,
    )
    first_run_id = handle.result_run_id
    await harness.workflows.submit(cmd(conv, "send", "m1", content="one"))
    await harness.wait_for(lambda s: s.status == "working")
    await harness.workflows.submit(cmd(conv, "send", "m2", content="two"))
    await harness.workflows.submit(cmd(conv, "send", "m3", content="three"))
    gated.release()
    state = await harness.wait_for(lambda s: s.turn_count == 3 and s.status == "idle")
    assert state.run_number == 2 and state.pending == []
    describe = await harness.client.get_workflow_handle(f"conv:{conv}").describe()
    assert describe.run_id != first_run_id
    dup = await harness.workflows.submit(cmd(conv, "send", "m1", content="one"))
    assert dup.duplicate, "seen ids survive continue-as-new"
    assert [t.turn_id for t in harness.repo.turns] == ["turn-1", "turn-2", "turn-3"]


class FailOncePublisher:
    """Raises on the first publish (failing the model activity's first attempt), then works."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.failed = False

    async def publish(self, event: Any) -> str:
        if not self.failed and event.type == "text.delta":
            self.failed = True
            raise RuntimeError("simulated publisher outage")
        return await self.inner.publish(event)


async def test_model_activity_retry_meters_once(harness: Harness) -> None:
    from toy.billing import BillingModel, InMemoryBillingClient
    from toy.runtime import runtime

    billing = InMemoryBillingClient()
    harness.set_model(
        BillingModel(
            TestModel(call_tools=[], custom_output_text="metered reply"), billing, customer="cus_t"
        )
    )
    runtime.publisher = FailOncePublisher(harness.events)
    conv = harness.conversation_id
    await harness.workflows.submit(cmd(conv, "send", "m1", content="hi"))
    state = await harness.wait_for(lambda s: s.turn_count == 1 and s.status == "idle")
    assert state.last_terminal == TurnTerminal.COMPLETED
    await harness.wait_until(lambda: harness.event_types()[-1] == "status.changed")
    # Attempt 1 failed inside the stream, so it is a *partial* (attempt-suffixed) event; attempt 2
    # is the full completion. Each completion is metered exactly once; nothing is double-counted.
    assert len(billing.attempts) == 2 and len(billing.events) == 2
    partial, full = sorted(billing.events.values(), key=lambda e: e.attempt)
    assert partial.partial and partial.attempt == 1
    assert not full.partial and full.attempt == 2
    deltas = harness.events_of("text.delta")
    assert deltas and all(e.attempt == 2 for e in deltas)
    usage_events = harness.events_of("usage.updated")
    assert [e.attempt for e in usage_events] == [1, 2]
