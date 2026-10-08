"""Temporal worker: `uv run agent-worker`."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import httpx
from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
from pydantic_ai.models import Model
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.providers.litellm import LiteLLMProvider
from temporalio.client import Client
from temporalio.worker import Worker

from toy.activities import ConversationActivities
from toy.adapters.redis_events import RedisEventPublisher, make_redis
from toy.billing import BillingModel, HttpBillingClient
from toy.db import PostgresRepository, create_pool
from toy.runtime import runtime
from toy.settings import Settings, get_settings
from toy.workflow import ConversationWorkflow

log = logging.getLogger(__name__)

TEST_MODEL_NAME = "test"


def build_model(settings: Settings) -> Model:
    """`LLM_MODEL=test` gives Pydantic AI's `TestModel` (no key); otherwise LiteLLM."""
    if settings.llm_model == TEST_MODEL_NAME:
        log.warning("LLM_MODEL=test: using Pydantic AI TestModel, not a real LLM")
        return TestModel()
    provider = LiteLLMProvider(api_key=settings.litellm_api_key, api_base=settings.litellm_base_url)
    return OpenAIChatModel(settings.llm_model, provider=provider)


async def run_worker(settings: Settings) -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
        plugins=[PydanticAIPlugin()],
    )
    pool = await create_pool(settings.database_url)
    redis = make_redis(settings.redis_url)
    publisher = RedisEventPublisher(redis)
    http = httpx.AsyncClient(timeout=10)
    billing = HttpBillingClient(http, settings.billing_url)
    runtime.model = BillingModel(
        build_model(settings),
        billing,
        customer=settings.billing_customer,
        price_model_name=settings.llm_model,
    )
    runtime.publisher = publisher
    activities = ConversationActivities(PostgresRepository(pool), publisher)
    worker = Worker(
        client,
        task_queue=settings.task_queue,
        workflows=[ConversationWorkflow],
        activities=activities.all,
        max_concurrent_activities=10,
        graceful_shutdown_timeout=timedelta(seconds=60),
    )
    log.info(
        "worker on %s (model=%s, billing=%s)",
        settings.task_queue,
        runtime.model.model_name,
        settings.billing_url,
    )
    try:
        await worker.run()
    finally:
        await pool.close()
        await redis.aclose()
        await http.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_worker(get_settings()))


if __name__ == "__main__":
    main()
