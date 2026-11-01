"""Bounded and serialized TCP transactions, independent of Home Assistant."""
from __future__ import annotations
import asyncio
import json
import re

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
    def __init__(self, host, port=80, *, timeout=5, settle=0.5):
        self.host, self.port = host, port
        self.timeout, self.settle = timeout, settle
        self._lock = asyncio.Lock()

    async def request(self, payload):
        validate_request(payload)
        # Cancellation while queued never releases another transaction's lock.
        async with self._lock:
            return await asyncio.wait_for(self._exchange(payload), self.timeout)

    async def read_registers(self, commands):
        """Split only explicit A/I reads, validating the complete collection first.

        Each chunk owns one normal transaction. No writes or retries can be
        introduced here, and partial collections are never returned on failure.
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
            response = validate_response(await self.request(payload))
            values = decode_registers(response)
            expected = {item[1:6]: {"A": "B", "I": "J"}[item[0]] for item in chunk}
            if (response[0] != "SEC" or len(response) - 2 != len(chunk) or
                    set(values) != set(expected) or any(
                    values[key][1] != marker for key, marker in expected.items())):
                raise ProtocolError("Explicit read response does not match request")
            result.update(values)
        return result

    async def _exchange(self, payload):
        writer = None
        try:
            reader, writer = await asyncio.open_connection(self.host, self.port)
            if self.settle:
                await asyncio.sleep(self.settle)
            writer.write(payload)
            await writer.drain()
            buffer = bytearray()
            while len(buffer) < MAX_RESPONSE_BYTES:
                chunk = await reader.read(min(4096, MAX_RESPONSE_BYTES-len(buffer)))
                if not chunk:
                    raise ProtocolError("Connection closed before complete response")
                buffer.extend(chunk)
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
