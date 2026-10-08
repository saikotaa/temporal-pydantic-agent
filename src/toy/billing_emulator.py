"""Stripe-like meter events emulator: `uv run python -m toy.billing_emulator` (port 8402).

Stores events in memory; `POST /v1/billing/meter_events` returns 409 on a duplicate identifier.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class MeterEventIn(BaseModel):
    event_name: str
    identifier: str
    customer: str
    value: int
    model_config = {"extra": "allow"}


class MeterEventOut(BaseModel):
    object: str = "billing.meter_event"
    event_name: str
    identifier: str
    customer: str
    value: int
    created: int
    metadata: dict[str, Any] = Field(default_factory=dict[str, Any])


def create_emulator() -> FastAPI:
    app = FastAPI(title="stripe-meter-emulator")
    store: dict[str, MeterEventOut] = {}

    @app.post("/v1/billing/meter_events", status_code=200)
    async def create_meter_event(event: MeterEventIn) -> MeterEventOut:
        if event.identifier in store:
            raise HTTPException(status_code=409, detail="duplicate identifier")
        out = MeterEventOut(
            event_name=event.event_name,
            identifier=event.identifier,
            customer=event.customer,
            value=event.value,
            created=int(datetime.now(UTC).timestamp()),
            metadata=dict(event.model_extra or {}),
        )
        store[event.identifier] = out
        return out

    @app.get("/v1/billing/meter_events")
    async def list_meter_events(customer: str | None = None) -> dict[str, Any]:
        events = [e for e in store.values() if customer is None or e.customer == customer]
        return {"object": "list", "data": events, "total_value": sum(e.value for e in events)}

    @app.delete("/v1/billing/meter_events", status_code=204)
    async def reset() -> None:
        store.clear()

    return app


def main() -> None:
    port = int(os.environ.get("BILLING_PORT", "8402"))
    uvicorn.run(create_emulator(), host="0.0.0.0", port=port, log_level="warning")


if __name__ == "__main__":
    main()
