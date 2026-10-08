"""Temporal implementation of `ConversationWorkflowClient`.

`send`-kind commands (send, steer, context) use update-with-start on workflow id
`conv:{conversation_id}` so the first message creates the workflow; `tool_results` is an update
on the existing workflow; edit/delete/reorder/send_now/stop are signals; state is a query.
"""

from __future__ import annotations

from temporalio.client import Client, WithStartWorkflowOperation
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import TemporalError
from temporalio.service import RPCError, RPCStatusCode

from toy.commands import Command, CommandAck, CommandKind
from toy.settings import Settings
from toy.state import ConversationState, ConversationStatus
from toy.workflow import SIGNALS, ConversationWorkflow, WorkflowInput

CREATING_KINDS = frozenset({CommandKind.SEND, CommandKind.STEER, CommandKind.CONTEXT})


def workflow_id_for(conversation_id: str) -> str:
    return f"conv:{conversation_id}"


def _not_found(exc: BaseException) -> bool:
    return isinstance(exc, RPCError) and exc.status == RPCStatusCode.NOT_FOUND


class TemporalWorkflowClient:
    def __init__(self, client: Client, task_queue: str) -> None:
        self._client = client
        self._task_queue = task_queue

    async def submit(self, command: Command) -> CommandAck:
        workflow_id = workflow_id_for(command.conversation_id)
        kind = command.payload.kind
        if kind in CREATING_KINDS:
            start_op = WithStartWorkflowOperation(
                ConversationWorkflow.run,
                WorkflowInput(conversation_id=command.conversation_id),
                id=workflow_id,
                task_queue=self._task_queue,
                id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
            )
            ack = await self._client.execute_update_with_start_workflow(  # pyright: ignore[reportUnknownMemberType]
                ConversationWorkflow.send, command, start_workflow_operation=start_op
            )
            ack.workflow_id = workflow_id
            return ack

        handle = self._client.get_workflow_handle_for(ConversationWorkflow.run, workflow_id)
        try:
            if kind == CommandKind.TOOL_RESULTS:
                ack = await handle.execute_update(  # pyright: ignore[reportUnknownMemberType]
                    ConversationWorkflow.send, command
                )
                ack.workflow_id = workflow_id
                return ack
            await handle.signal(SIGNALS[kind], command.payload)
            state = await handle.query(ConversationWorkflow.state)
        except TemporalError as exc:
            if _not_found(exc) or _not_found(exc.__cause__ or exc):
                return CommandAck(
                    accepted=False, status=ConversationStatus.IDLE, detail="conversation not found"
                )
            raise
        return CommandAck(accepted=True, status=state.status, workflow_id=workflow_id)

    async def get_state(self, conversation_id: str) -> ConversationState | None:
        handle = self._client.get_workflow_handle_for(
            ConversationWorkflow.run, workflow_id_for(conversation_id)
        )
        try:
            return await handle.query(ConversationWorkflow.state)
        except TemporalError as exc:
            if _not_found(exc) or _not_found(exc.__cause__ or exc):
                return None
            raise


async def connect_temporal_client(settings: Settings) -> TemporalWorkflowClient:
    from pydantic_ai.durable_exec.temporal import PydanticAIPlugin

    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
        plugins=[PydanticAIPlugin()],
    )
    return TemporalWorkflowClient(client, settings.task_queue)
