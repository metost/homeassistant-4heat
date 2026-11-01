"""Exercise real TCP streams against a simulated stove, never real writes."""
import asyncio
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("transport", ROOT / "custom_components/4heat/transport.py")
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
REPLY = ["SEL", "3", "J30001000000000005", "J30002000000000000", "J30017000000000065"]


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.handlers = set()
        self.servers = []

    async def asyncTearDown(self):
        for server in self.servers:
            server.close()
            await server.wait_closed()
        for task in self.handlers:
            task.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)

    async def client(self, handler, timeout=0.2):
        async def tracked(reader, writer):
            task = asyncio.current_task()
            self.handlers.add(task)
            try:
                await handler(reader, writer)
            finally:
                writer.close()
                await writer.wait_closed()
                self.handlers.discard(task)
        server = await asyncio.start_server(tracked, "127.0.0.1", 0)
        self.servers.append(server)
        return transport.FourHeatTransport("127.0.0.1", server.sockets[0].getsockname()[1],
                                           settle=0, timeout=timeout)

    async def test_fragmented_response(self):
        async def handler(r, w):
            await r.read(100)
            raw = json.dumps(REPLY).encode()
            for start in range(0, len(raw), 7):
                w.write(raw[start:start+7])
                await w.drain()
                await asyncio.sleep(0.001)
        c = await self.client(handler)
        self.assertEqual(await c.request(b'["SEL","0"]'), REPLY)

    async def test_timeout_closes_connection_and_next_request_works(self):
        closed = asyncio.Event()
        count = 0
        async def handler(r, w):
            nonlocal count
            count += 1
            await r.read(100)
            if count == 1:
                await r.read()
                closed.set()
            else:
                w.write(json.dumps(REPLY).encode())
        c = await self.client(handler, timeout=0.03)
        with self.assertRaises(asyncio.TimeoutError):
            await c.request(b"read")
        await asyncio.wait_for(closed.wait(), 0.2)
        self.assertEqual(await c.request(b"read"), REPLY)

    async def test_reads_and_commands_do_not_overlap(self):
        active = peak = 0
        received = []
        async def handler(r, w):
            nonlocal active, peak
            received.append(await r.read(100))
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            w.write(json.dumps(REPLY).encode())
        c = await self.client(handler)
        await asyncio.gather(c.request(b"read"), c.request(b"command"), c.request(b"read"))
        self.assertEqual(peak, 1)
        self.assertEqual(received, [b"read", b"command", b"read"])

    async def test_cancelled_transaction_releases_socket(self):
        entered, closed = asyncio.Event(), asyncio.Event()
        async def handler(r, w):
            await r.read(100)
            entered.set()
            await r.read()
            closed.set()
        c = await self.client(handler)
        task = asyncio.create_task(c.request(b"read"))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(closed.wait(), 0.2)
        self.assertFalse(c._lock.locked())

    async def test_truncated_response(self):
        async def handler(r, w):
            await r.read(100)
            w.write(b'["SEL","3"')
        c = await self.client(handler)
        with self.assertRaises(transport.ProtocolError):
            await c.request(b"read")

    def test_envelope_and_register_validation(self):
        for bad in [None, [], ["SEL", "3"], ["BOGUS", "0"], ["SEC", 0]]:
            with self.subTest(bad=bad), self.assertRaises(transport.ProtocolError):
                transport.validate_response(bad)
        for bad in [["ERR", "0"], ["SEL", "1", "bad"],
                    ["SEL", "2", REPLY[2], "J30001000000000006"]]:
            with self.subTest(bad=bad), self.assertRaises(transport.ProtocolError):
                transport.decode_registers(bad)
        self.assertEqual(transport.decode_registers(REPLY)["30017"], [65, "J"])
        self.assertEqual(transport.decode_registers(["SEL", "2", REPLY[2], REPLY[2]]),
                         {"30001": [5, "J"]})

    def test_anonymized_real_packet_shape(self):
        packet = json.loads((ROOT/'tests/status_fixture.json').read_text())
        result = transport.decode_registers(transport.validate_response(packet))
        self.assertEqual(int(packet[1]), 18)
        self.assertEqual(len(result), 17)
