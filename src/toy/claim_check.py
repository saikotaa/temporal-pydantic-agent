"""Claim-check `PayloadCodec`: large Temporal payloads go to S3, the history keeps a key.

`TemporalDurability` sends the full `message_history` into every model activity, so payloads
grow with the conversation while Temporal caps a single payload at 2 MB and a run's history at
50 MB / 51 200 events. The codec gzips any payload over `threshold` bytes, stores it under its
sha256 (content-addressed, so a retried activity or a replayed history reuses the object) and
replaces the payload with a small claim. Decoding a claim whose object is gone is a
non-retryable `ApplicationError`: no retry can bring the data back.

`ClaimCheckPlugin` swaps only the `payload_codec` on whatever data converter the client already
has, so Pydantic AI's payload converter (installed by `PydanticAIPlugin`) stays in place.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gzip
import hashlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.exceptions import ApplicationError
from temporalio.plugin import SimplePlugin

from toy.settings import Settings

log = logging.getLogger(__name__)

ENCODING = b"binary/claim-check"
METADATA_ENCODING = "encoding"
METADATA_KEY = "claim-check-key"
METADATA_SIZE = "claim-check-original-size"
METADATA_STORED_SIZE = "claim-check-stored-size"
DEFAULT_THRESHOLD = 20 * 1024
CLAIM_MISSING_ERROR = "ClaimCheckMissing"


class ClaimStore(Protocol):
    async def put_if_absent(self, key: str, data: bytes) -> bool:
        """Store `data` under `key` unless present. Returns True if it was written now."""
        ...

    async def get(self, key: str) -> bytes | None: ...


@dataclass
class InMemoryClaimStore:
    objects: dict[str, bytes] = field(default_factory=dict[str, bytes])
    puts: int = 0

    async def put_if_absent(self, key: str, data: bytes) -> bool:
        if key in self.objects:
            return False
        self.puts += 1
        self.objects[key] = data
        return True

    async def get(self, key: str) -> bytes | None:
        return self.objects.get(key)


class S3ClaimStore:
    """boto3-backed store (MinIO or any S3 API); calls run in a thread."""

    def __init__(self, client: Any, bucket: str, *, prefix: str = "claims/") -> None:
        self._s3 = client
        self._bucket = bucket
        self._prefix = prefix

    @classmethod
    def from_settings(cls, settings: Settings) -> S3ClaimStore:
        import boto3
        from botocore.config import Config

        client: Any = boto3.client(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )
        return cls(client, settings.s3_bucket)

    def ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=self._bucket)
        except Exception:
            self._s3.create_bucket(Bucket=self._bucket)

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def _exists(self, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=self._bucket, Key=self._key(key))
        except Exception:
            return False
        return True

    async def put_if_absent(self, key: str, data: bytes) -> bool:
        if await asyncio.to_thread(self._exists, key):
            return False
        await asyncio.to_thread(
            self._s3.put_object, Bucket=self._bucket, Key=self._key(key), Body=data
        )
        return True

    async def get(self, key: str) -> bytes | None:
        def read() -> bytes | None:
            try:
                obj = self._s3.get_object(Bucket=self._bucket, Key=self._key(key))
            except Exception:
                return None
            return obj["Body"].read()

        return await asyncio.to_thread(read)


@dataclass
class CodecStats:
    offloaded: int = 0
    inline: int = 0
    original_bytes: int = 0
    stored_bytes: int = 0


class ClaimCheckCodec(PayloadCodec):
    def __init__(self, store: ClaimStore, *, threshold: int = DEFAULT_THRESHOLD) -> None:
        self._store = store
        self._threshold = threshold
        self.stats = CodecStats()

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for payload in payloads:
            if (
                len(payload.data) < self._threshold
                or payload.metadata.get(METADATA_ENCODING) == ENCODING
            ):
                self.stats.inline += 1
                out.append(payload)
                continue
            raw = payload.SerializeToString()
            blob = gzip.compress(raw, compresslevel=6)
            key = hashlib.sha256(blob).hexdigest()
            await self._store.put_if_absent(key, blob)
            self.stats.offloaded += 1
            self.stats.original_bytes += len(raw)
            self.stats.stored_bytes += len(blob)
            out.append(
                Payload(
                    metadata={
                        METADATA_ENCODING: ENCODING,
                        METADATA_KEY: key.encode(),
                        METADATA_SIZE: str(len(raw)).encode(),
                        METADATA_STORED_SIZE: str(len(blob)).encode(),
                    },
                    data=key.encode(),
                )
            )
        return out

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for payload in payloads:
            if payload.metadata.get(METADATA_ENCODING) != ENCODING:
                out.append(payload)
                continue
            key = payload.data.decode()
            blob = await self._store.get(key)
            if blob is None:
                raise ApplicationError(
                    f"claim-check object {key} is missing from the store",
                    type=CLAIM_MISSING_ERROR,
                    non_retryable=True,
                )
            restored = Payload()
            restored.ParseFromString(gzip.decompress(blob))
            out.append(restored)
        return out


class ClaimCheckPlugin(SimplePlugin):
    """Installs the codec on the client (and the workers it creates) keeping the converter."""

    def __init__(self, codec: ClaimCheckCodec) -> None:
        def with_codec(converter: DataConverter | None) -> DataConverter:
            base = converter or DataConverter.default
            if base.payload_codec is not None:
                log.warning("replacing existing payload codec %r", base.payload_codec)
            return dataclasses.replace(base, payload_codec=codec)

        super().__init__(  # pyright: ignore[reportUnknownMemberType]
            name="ClaimCheckPlugin", data_converter=with_codec
        )
