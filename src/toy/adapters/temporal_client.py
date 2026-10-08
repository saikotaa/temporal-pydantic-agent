"""Temporal implementation of `ConversationWorkflowClient`. Filled in at step 5."""

from __future__ import annotations

from toy.ports import ConversationWorkflowClient
from toy.settings import Settings


async def connect_temporal_client(settings: Settings) -> ConversationWorkflowClient:
    raise NotImplementedError("step 5 wires the Temporal client")
