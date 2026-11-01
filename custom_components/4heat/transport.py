"""Bounded and serialized TCP transactions, independent of Home Assistant."""
from __future__ import annotations
import asyncio
import json
import re

MAX_RESPONSE_BYTES = 65536
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


class FourHeatTransport:
    def __init__(self, host, port=80, *, timeout=5, settle=0.5):
        self.host, self.port = host, port
        self.timeout, self.settle = timeout, settle
        self._lock = asyncio.Lock()

    async def request(self, payload):
        # Cancellation while queued never releases another transaction's lock.
        async with self._lock:
            return await asyncio.wait_for(self._exchange(payload), self.timeout)

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
