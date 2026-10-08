"""One long-lived `ConversationWorkflow` per conversation.

Holds the editable queue in workflow memory, runs the Pydantic AI agent inside the workflow
(every model request / tool call becomes an activity through `TemporalDurability`), races each
turn against the stop flag, persists through activities, and continues-as-new every
`TURNS_PER_RUN` turns carrying the queue and counters.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from pydantic import BaseModel
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from pydantic_ai import DeferredToolRequests, DeferredToolResults, ToolApproved, ToolDenied
    from pydantic_ai.durable_exec.temporal import PydanticAIWorkflow
    from pydantic_ai.messages import ModelMessage

    from toy.activities import ConversationActivities, MirrorStatusInput, PersistTurnInput
    from toy.adapters.memory import apply_queue_command
    from toy.agent import conversation_agent
    from toy.commands import (
        Command,
        CommandAck,
        CommandKind,
        DeletePayload,
        EditPayload,
        ReorderPayload,
        SendNowPayload,
        StopPayload,
    )
    from toy.events import EventType, StreamEvent
    from toy.state import (
        ConversationState,
        ConversationStatus,
        PendingApproval,
        QueuedKind,
        QueuedMessage,
        TurnTerminal,
    )
    from toy.stream_events import ConversationDeps

TURNS_PER_RUN = 20
SEEN_IDS_LIMIT = 1000
IO_ACTIVITY_TIMEOUT = timedelta(seconds=30)


class WorkflowInput(BaseModel):
    """Start argument; also the continue-as-new carry."""

    conversation_id: str
    pending: list[QueuedMessage] = []
    turn_count: int = 0
    run_number: int = 0
    status: ConversationStatus = ConversationStatus.IDLE
    pending_approvals: list[PendingApproval] = []
    last_terminal: TurnTerminal | None = None
    seen_ids: list[str] = []
    turns_per_run: int = TURNS_PER_RUN


@workflow.defn
class ConversationWorkflow(PydanticAIWorkflow):
    __pydantic_ai_agents__ = [conversation_agent]

    def __init__(self) -> None:
        self.conversation_id = ""
        self._state = ConversationState()
        self.inbox: list[str] = []
        self.cancel_requested = False
        self.seen_ids: list[str] = []
        self.history: list[ModelMessage] = []
        self.turns_this_run = 0
        self.turns_per_run = TURNS_PER_RUN

    # --- lifecycle -----------------------------------------------------------------------------

    @workflow.run
    async def run(self, data: WorkflowInput) -> None:
        # Handlers (update-with-start) may already have run before this method starts, so the
        # carried state is merged in front of anything they queued rather than replacing it.
        self.conversation_id = data.conversation_id
        self.turns_per_run = data.turns_per_run
        self.seen_ids = [*data.seen_ids, *self.seen_ids]
        self._state = ConversationState(
            status=data.status,
            pending=[*data.pending, *self._state.pending],
            turn_count=data.turn_count,
            pending_approvals=list(data.pending_approvals),
            last_terminal=data.last_terminal,
            run_number=data.run_number + 1,
        )
        self.history = await workflow.execute_activity_method(
            ConversationActivities.load_history,
            self.conversation_id,
            start_to_close_timeout=IO_ACTIVITY_TIMEOUT,
        )
        await self._mirror()
        while True:
            await workflow.wait_condition(self._has_runnable_message)
            message = self._state.pending.pop(0)
            await self._run_turn(message)
            if self.turns_this_run >= self.turns_per_run:
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(self._carry())

    def _has_runnable_message(self) -> bool:
        if not self._state.pending:
            return False
        if self._state.status == ConversationStatus.BLOCKED:
            return self._state.pending[0].kind == QueuedKind.TOOL_RESULTS
        return True

    def _carry(self) -> WorkflowInput:
        return WorkflowInput(
            conversation_id=self.conversation_id,
            pending=self._state.pending,
            turn_count=self._state.turn_count,
            run_number=self._state.run_number,
            status=self._state.status,
            pending_approvals=self._state.pending_approvals,
            last_terminal=self._state.last_terminal,
            seen_ids=self.seen_ids[-SEEN_IDS_LIMIT:],
            turns_per_run=self.turns_per_run,
        )

    # --- handlers: mutate state only -----------------------------------------------------------

    @workflow.update
    async def send(self, command: Command) -> CommandAck:
        if command.client_message_id in self.seen_ids:
            return CommandAck(accepted=True, status=self._state.status, duplicate=True)
        kind = command.payload.kind
        if (
            kind in (CommandKind.STEER, CommandKind.CONTEXT)
            and self._state.status == ConversationStatus.WORKING
        ):
            content = getattr(command.payload, "content", "")
            label = "Steering instruction" if kind == CommandKind.STEER else "Additional context"
            self.inbox.append(f"[{label} from the user]: {content}")
            self._remember(command.client_message_id)
            return CommandAck(accepted=True, status=self._state.status, detail="inbox")
        ack = apply_queue_command(self._state, command)
        if ack.accepted:
            self._remember(command.client_message_id)
        return ack

    @workflow.signal
    def edit(self, payload: EditPayload) -> None:
        for m in self._state.pending:
            if m.client_message_id == payload.target_client_message_id:
                m.content = payload.content

    @workflow.signal
    def delete(self, payload: DeletePayload) -> None:
        self._state.pending = [
            m
            for m in self._state.pending
            if m.client_message_id != payload.target_client_message_id
        ]

    @workflow.signal
    def reorder(self, payload: ReorderPayload) -> None:
        from toy.adapters.memory import reorder_pending

        self._state.pending = reorder_pending(self._state.pending, payload.order)

    @workflow.signal
    def send_now(self, payload: SendNowPayload) -> None:
        from toy.adapters.memory import reorder_pending

        self._state.pending = reorder_pending(
            self._state.pending, [payload.target_client_message_id]
        )

    @workflow.signal
    def stop(self, payload: StopPayload) -> None:
        if self._state.status != ConversationStatus.WORKING:
            return
        if payload.expected_turn_id and payload.expected_turn_id != self._state.current_turn_id:
            return  # scoped cancel: a newer turn is running
        self.cancel_requested = True

    @workflow.query
    def state(self) -> ConversationState:
        return self._state.model_copy(deep=True)

    def drain_inbox(self) -> list[str]:
        """Called by the agent's history processor (workflow code) before each model request."""
        lines, self.inbox = self.inbox, []
        return lines

    def _remember(self, client_message_id: str) -> None:
        self.seen_ids.append(client_message_id)
        if len(self.seen_ids) > SEEN_IDS_LIMIT:
            del self.seen_ids[: len(self.seen_ids) - SEEN_IDS_LIMIT]

    # --- turn ----------------------------------------------------------------------------------

    async def _run_turn(self, message: QueuedMessage) -> None:
        self._state.turn_count += 1
        self.turns_this_run += 1
        turn_id = f"turn-{self._state.turn_count}"
        self._state.status = ConversationStatus.WORKING
        self._state.current_turn_id = turn_id
        self._state.current_client_message_id = message.client_message_id
        self.cancel_requested = False
        self.inbox = []
        await self._publish(
            turn_id, EventType.TURN_STARTED, client_message_id=message.client_message_id
        )
        await self._mirror()

        prompt: str | None
        deferred: DeferredToolResults | None = None
        if message.kind == QueuedKind.TOOL_RESULTS:
            prompt = None
            deferred = DeferredToolResults(
                approvals={
                    r.tool_call_id: ToolApproved()
                    if r.approved
                    else ToolDenied(message=r.reason or "The user declined this action.")
                    for r in message.tool_results
                }
            )
            await self._publish(
                turn_id,
                EventType.REQUEST_RESOLVED,
                approvals=[
                    {"tool_call_id": r.tool_call_id, "approved": r.approved, "reason": r.reason}
                    for r in message.tool_results
                ],
            )
            self._state.pending_approvals = []
        elif message.kind == QueuedKind.SEND:
            prompt = message.content
        else:
            prompt = f"[{message.kind.value} from the user]: {message.content}"

        deps = ConversationDeps(conversation_id=self.conversation_id, turn_id=turn_id)
        run_task = asyncio.create_task(
            conversation_agent.run(
                prompt,
                deps=deps,
                message_history=list(self.history),
                deferred_tool_results=deferred,
            )
        )
        stop_task = asyncio.create_task(workflow.wait_condition(lambda: self.cancel_requested))
        done, _ = await workflow.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

        terminal = TurnTerminal.COMPLETED
        error: str | None = None
        usage: dict[str, Any] | None = None
        approvals: list[dict[str, Any]] | None = None
        if run_task not in done:
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await run_task
            terminal = TurnTerminal.STOPPED
        else:
            stop_task.cancel()
            try:
                result = run_task.result()
            except Exception as exc:  # a failed turn must not fail the workflow
                terminal = TurnTerminal.FAILED
                error = f"{type(exc).__name__}: {exc}"
                workflow.logger.warning("turn %s failed: %s", turn_id, error)
            else:
                new_messages = result.new_messages()
                self.history = result.all_messages()
                run_usage = result.usage
                usage = {
                    "input_tokens": run_usage.input_tokens,
                    "output_tokens": run_usage.output_tokens,
                    "cache_read_tokens": run_usage.cache_read_tokens,
                    "cache_write_tokens": run_usage.cache_write_tokens,
                    "requests": run_usage.requests,
                    "tool_calls": run_usage.tool_calls,
                }
                if isinstance(result.output, DeferredToolRequests):
                    terminal = TurnTerminal.BLOCKED
                    self._state.pending_approvals = [
                        PendingApproval(
                            tool_call_id=call.tool_call_id,
                            tool_name=call.tool_name,
                            args=call.args_as_dict(),
                        )
                        for call in result.output.approvals
                    ]
                    approvals = [a.model_dump() for a in self._state.pending_approvals]
                await workflow.execute_activity_method(
                    ConversationActivities.persist_turn,
                    PersistTurnInput(
                        conversation_id=self.conversation_id,
                        turn_id=turn_id,
                        messages=new_messages,
                        terminal=terminal,
                        usage=usage,
                    ),
                    start_to_close_timeout=IO_ACTIVITY_TIMEOUT,
                )

        self._state.status = (
            ConversationStatus.BLOCKED
            if terminal == TurnTerminal.BLOCKED
            else ConversationStatus.IDLE
        )
        self._state.last_terminal = terminal
        self._state.current_turn_id = None
        self._state.current_client_message_id = None
        self.inbox = []
        await self._publish(
            turn_id,
            EventType.TURN_COMPLETED,
            terminal=terminal,
            status=self._state.status,
            error=error,
            usage=usage,
            approvals=approvals,
            client_message_id=message.client_message_id,
        )
        await self._mirror()

    # --- I/O through activities -----------------------------------------------------------------

    async def _publish(self, turn_id: str, type_: EventType, **fields: Any) -> None:
        await workflow.execute_activity_method(
            ConversationActivities.publish_event,
            StreamEvent(
                conversation_id=self.conversation_id, turn_id=turn_id, type=type_, **fields
            ),
            start_to_close_timeout=IO_ACTIVITY_TIMEOUT,
        )

    async def _mirror(self) -> None:
        await workflow.execute_activity_method(
            ConversationActivities.mirror_status,
            MirrorStatusInput(
                conversation_id=self.conversation_id,
                status=self._state.status,
                pending_count=len(self._state.pending),
                turn_count=self._state.turn_count,
            ),
            start_to_close_timeout=IO_ACTIVITY_TIMEOUT,
        )
        await self._publish(
            self._state.current_turn_id or f"turn-{self._state.turn_count}",
            EventType.STATUS_CHANGED,
            status=self._state.status,
            pending_count=len(self._state.pending),
        )


SignalMethod = Callable[..., Any]
SIGNALS: dict[CommandKind, SignalMethod] = {
    CommandKind.EDIT: ConversationWorkflow.edit,
    CommandKind.DELETE: ConversationWorkflow.delete,
    CommandKind.REORDER: ConversationWorkflow.reorder,
    CommandKind.SEND_NOW: ConversationWorkflow.send_now,
    CommandKind.STOP: ConversationWorkflow.stop,
}
