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
READ = b'["SEL","0"]'
WRITE = b'["SEC","1","J30254000000000001"]'
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
                                           settle=0, timeout=timeout, transaction_gap=0, read_retry_delay=0)

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
            await c.request(READ)
        await asyncio.wait_for(closed.wait(), 0.2)
        self.assertEqual(await c.request(READ), REPLY)

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
        await asyncio.gather(c.request(READ), c.request(WRITE), c.request(READ))
        self.assertEqual(peak, 1)
        self.assertEqual(received, [READ, WRITE, READ])

    async def test_cancelled_transaction_releases_socket(self):
        entered, closed = asyncio.Event(), asyncio.Event()
        async def handler(r, w):
            await r.read(100)
            entered.set()
            await r.read()
            closed.set()
        c = await self.client(handler)
        task = asyncio.create_task(c.request(READ))
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
            await c.request(READ)

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

    async def test_outbound_limit_rejects_writes_without_sending(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        sent = []
        async def exchange(payload):
            sent.append(payload)
            return ['SEC', '0']
        c._exchange = exchange
        item = 'B20180000000000065'
        for count in (15, 50):
            with self.assertRaises(transport.ProtocolError):
                await c.request(json.dumps(['SEC', str(count), *([item] * count)]).encode())
        self.assertEqual(sent, [])
        await c.request(json.dumps(['SEC', '14', *([item] * 14)]).encode())
        self.assertEqual(len(sent), 1)
        # The outbound cap never applies to cached SEL responses.
        packet = json.loads((ROOT/'tests/status_fixture.json').read_text())
        transport.validate_response(packet)

    async def test_large_read_collection_splits_at_14_without_writes(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        chunks = []
        async def request(payload):
            message = transport.validate_request(payload)
            chunks.append(message[2:])
            return ['SEC', message[1], *('J' + item[1:] for item in message[2:])]
        c.request = request
        commands = [f'I{30000 + i:05d}000000000000' for i in range(50)]
        result = await c.read_registers(commands)
        self.assertEqual([len(chunk) for chunk in chunks], [14, 14, 14, 8])
        self.assertEqual([item for chunk in chunks for item in chunk], commands)
        self.assertEqual(len(result), 50)
        chunks.clear()
        for bad in [commands + ['B20180000000000065'], commands + [commands[0]],
                    ['I30001000000000001'], []]:
            with self.assertRaises(transport.ProtocolError):
                await c.read_registers(bad)
        self.assertEqual(chunks, [])

    async def test_explicit_read_rejects_wrong_fields_with_bounded_retry(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        for response in [['SEL', '1', 'J30001000000000005'],
                         ['SEC', '1', 'J30002000000000000'],
                         ['SEC', '1', 'B30001000000000005'], ['SEC', '0']]:
            sent = []
            async def request(payload):
                sent.append(payload)
                return response
            c.request = request
            with self.assertRaises(transport.ProtocolError):
                await c.read_registers(['I30001000000000000'])
            self.assertEqual(len(sent), 2)

    async def test_cancelled_waiter_does_not_release_owners_lock(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        entered, release = asyncio.Event(), asyncio.Event()
        async def exchange(payload):
            entered.set()
            await release.wait()
            return REPLY
        c._exchange = exchange
        owner = asyncio.create_task(c.request(READ))
        await entered.wait()
        waiter = asyncio.create_task(c.request(WRITE))
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertTrue(c._lock.locked())
        release.set()
        await owner
        self.assertFalse(c._lock.locked())

    async def test_later_read_chunk_failure_returns_no_partial_result_or_retry(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        calls = []
        async def request(payload):
            message = transport.validate_request(payload)
            calls.append(message)
            if message[2][1:6] == '30014':
                raise asyncio.TimeoutError()
            return ['SEC', message[1], *('J' + item[1:] for item in message[2:])]
        c.request = request
        commands = [f'I{30000 + i:05d}000000000000' for i in range(40)]
        with self.assertRaises(asyncio.TimeoutError):
            await c.read_registers(commands)
        self.assertEqual(len(calls), 3)

    async def test_explicit_response_with_extra_duplicate_is_rejected(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        async def request(payload):
            return ['SEC', '2', REPLY[2], REPLY[2]]
        c.request = request
        with self.assertRaises(transport.ProtocolError):
            await c.read_registers(['I30001000000000000'])

    async def test_read_retry_recovers_from_timeout_without_retrying_writes(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        calls = []
        async def request(payload):
            calls.append(payload)
            if len(calls) == 1:
                raise asyncio.TimeoutError()
            return REPLY
        c.request = request
        self.assertEqual(await c.request_read(READ), REPLY)
        self.assertEqual(calls, [READ, READ])
        self.assertEqual(c.read_retries, 1)
        calls.clear()
        for write in [WRITE, b'["SEC","1","B20180000000000000"]', b'["SEC","1","0"]']:
            with self.assertRaises(transport.ProtocolError):
                await c.request_read(write)
        self.assertEqual(calls, [])

    async def test_malformed_read_is_validated_before_retry_success(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        calls = []
        async def request(payload):
            calls.append(payload)
            return ['SEL', '1', 'bad'] if len(calls) == 1 else REPLY
        c.request = request
        result = await c.request_read(READ, decoder=transport.decode_registers)
        self.assertEqual(result['30017'], [65, 'J'])
        self.assertEqual(len(calls), 2)

    async def test_read_retry_stops_after_two_failures(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=0)
        calls = []
        async def request(payload):
            calls.append(payload)
            raise OSError('disconnected')
        c.request = request
        with self.assertRaises(OSError):
            await c.request_read(READ)
        self.assertEqual(calls, [READ, READ])

    async def test_commands_proceed_during_read_retry_delay_and_cancellation_stops_retry(self):
        c = transport.FourHeatTransport('unused', transaction_gap=0, read_retry_delay=60)
        entered = asyncio.Event()
        calls = []
        async def exchange(payload):
            calls.append(payload)
            if payload == READ:
                entered.set()
                raise OSError('lost read')
            return ['SEC', '1', 'J30254000000000001']
        c._exchange = exchange
        read = asyncio.create_task(c.request_read(READ))
        await entered.wait()
        await asyncio.sleep(0)
        self.assertFalse(c._lock.locked())
        await asyncio.wait_for(c.request(WRITE), .2)
        read.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await read
        self.assertEqual(calls, [READ, WRITE])

    async def test_transaction_gap_spaces_successful_connections(self):
        c = transport.FourHeatTransport('unused', transaction_gap=.03, read_retry_delay=0)
        starts = []
        async def exchange(payload):
            starts.append(asyncio.get_running_loop().time())
            return REPLY
        c._exchange = exchange
        await asyncio.gather(c.request(READ), c.request(WRITE), c.request(READ))
        self.assertTrue(all(b-a >= .025 for a, b in zip(starts, starts[1:])))
