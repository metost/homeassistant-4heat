"""Bounded and serialized TCP transactions, independent of Home Assistant."""
from __future__ import annotations
import asyncio
import json
import logging
import re
from datetime import datetime, timezone

_LOGGER = logging.getLogger(__name__)

MAX_RESPONSE_BYTES = 65536
# Firmware 4.75 processes ordinary SEC queue indices 0..13. This cap applies
# to outbound explicit requests, never cached SEL response lengths.
MAX_SEC_ITEMS = 14
REGISTER = re.compile(r"[A-Za-z][0-9]{17}\Z")


class ProtocolError(ValueError):
    """Incomplete, malformed or rejected response."""


def validate_response(message):
    if not isinstance(message, list) or len(message) < 2:
        raise ProtocolError("Expected response array")
    if not all(isinstance(item, str) for item in message):
        raise ProtocolError("Expected string fields")
    if message[0] not in ("SEL", "SEC", "ERR"):
        raise ProtocolError("Unexpected response type")
    if not message[1].isdigit() or int(message[1]) != len(message) - 2:
        raise ProtocolError("Response count mismatch")
    return message


def decode_registers(message):
    """Preserve the existing integer values and [value, marker] shape."""
    validate_response(message)
    if message[0] not in ("SEL", "SEC"):
        raise ProtocolError("Status request rejected")
    data = {}
    for entry in message[2:]:
        if not REGISTER.fullmatch(entry):
            raise ProtocolError("Malformed register")
        key = entry[1:6]
        value = [int(entry[6:]), entry[0]]
        # Some real SEL packets repeat the same register (e.g. B20211).
        if key in data and data[key] != value:
            raise ProtocolError("Conflicting duplicate register")
        data[key] = value
    return data


def validate_request(payload):
    """Reject oversized SEC batches before opening a socket; never split writes."""
    try:
        message = json.loads(payload)
    except (ValueError, UnicodeError, TypeError) as err:
        raise ProtocolError("Malformed request JSON") from err
    validate_response(message)
    if message[0] == "SEL" and message[1] == "0":
        return message
    if message[0] != "SEC" or not 1 <= len(message) - 2 <= MAX_SEC_ITEMS:
        raise ProtocolError(f"SEC requests require 1..{MAX_SEC_ITEMS} items")
    return message


class FourHeatTransport:
    def __init__(self, host, port=80, *, timeout=5, settle=0.5,
                 transaction_gap=1, read_retry_delay=1):
        self.host, self.port = host, port
        self.timeout, self.settle = timeout, settle
        self._lock = asyncio.Lock()
        self.transaction_gap = transaction_gap
        self.read_retry_delay = read_retry_delay
        self._last_closed = float("-inf")
        self.read_retries = 0
        self.last_failure = None
        self.last_failure_at = None
        self._phase = "idle"
        self._received_bytes = 0

    async def request(self, payload):
        validate_request(payload)
        # Cancellation while queued never releases another transaction's lock.
        async with self._lock:
            loop = asyncio.get_running_loop()
            gap = self.transaction_gap - (loop.time() - self._last_closed)
            if gap > 0:
                await asyncio.sleep(gap)
            try:
                return await asyncio.wait_for(self._exchange(payload), self.timeout)
            except (OSError, asyncio.TimeoutError, ProtocolError) as err:
                self.last_failure = (f"{type(err).__name__} during {self._phase} "
                                     f"({self._received_bytes} response bytes): {err}").rstrip(": ")
                self.last_failure_at = datetime.now(timezone.utc)
                _LOGGER.debug("4heat transaction failed: %s", self.last_failure)
                raise
            finally:
                self._last_closed = loop.time()

    async def request_read(self, payload, *, decoder=None):
        """At most two validated reads. Commands can never enter this path.

        A failed attempt closes its socket before the retry delay. The delay
        owns no transport lock, allowing queued commands to proceed first.
        """
        message = validate_request(payload)
        if message[0] != "SEL" and any(
                not REGISTER.fullmatch(item) or item[0] not in ("A", "I") or
                item[6:] != "000000000000" for item in message[2:]):
            raise ProtocolError("Read retry path cannot send writes")
        for attempt in range(2):
            try:
                response = validate_response(await self.request(payload))
                return decoder(response) if decoder else response
            except (OSError, asyncio.TimeoutError, ProtocolError) as err:
                if attempt:
                    raise
                _LOGGER.debug("Retrying read once after %s: %s", type(err).__name__, err)
                await asyncio.sleep(self.read_retry_delay)
                self.read_retries += 1

    async def read_registers(self, commands):
        """Split only explicit A/I reads, validating the complete collection first.

        Each chunk uses the serialized transport. No writes can be
        introduced here. Each chunk allows one read-only retry; partial
        collections are never returned on failure.
        """
        commands = list(commands)
        if not commands or any(not isinstance(item, str) or
                not REGISTER.fullmatch(item) or item[0] not in ("A", "I") or
                item[6:] != "000000000000" for item in commands):
            raise ProtocolError("Expected zero-valued A/I register reads")
        if len({item[1:6] for item in commands}) != len(commands):
            raise ProtocolError("Duplicate read register")
        result = {}
        for start in range(0, len(commands), MAX_SEC_ITEMS):
            chunk = commands[start:start + MAX_SEC_ITEMS]
            payload = json.dumps(["SEC", str(len(chunk)), *chunk],
                                 separators=(",", ":")).encode()
            expected = {item[1:6]: {"A": "B", "I": "J"}[item[0]] for item in chunk}

            def decode_chunk(response):
                values = decode_registers(response)
                if (response[0] != "SEC" or len(response) - 2 != len(chunk) or
                        set(values) != set(expected) or any(
                        values[key][1] != marker for key, marker in expected.items())):
                    raise ProtocolError("Explicit read response does not match request")
                return values

            result.update(await self.request_read(payload, decoder=decode_chunk))
        return result

    async def _exchange(self, payload):
        writer = None
        try:
            self._phase = "connect"
            self._received_bytes = 0
            reader, writer = await asyncio.open_connection(self.host, self.port)
            if self.settle:
                self._phase = "connection settling"
                await asyncio.sleep(self.settle)
            self._phase = "send"
            writer.write(payload)
            await writer.drain()
            buffer = bytearray()
            self._phase = "receive"
            while len(buffer) < MAX_RESPONSE_BYTES:
                chunk = await reader.read(min(4096, MAX_RESPONSE_BYTES-len(buffer)))
                if not chunk:
                    raise ProtocolError("Connection closed before complete response")
                buffer.extend(chunk)
                self._received_bytes = len(buffer)
                try:
                    message = json.loads(buffer.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                return validate_response(message)
            raise ProtocolError("Response exceeds size limit")
        finally:
            if writer is not None:
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), 0.5)
                except (OSError, asyncio.TimeoutError):
                    pass
