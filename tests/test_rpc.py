import http.client
import io
import json
import time
import unittest

from fmbench import rpc


class FakeSocket:
    """A socket that answers its n-th request with `replies[n]`, or times out on `None`."""

    def __init__(self, *replies):
        self.replies = list(replies)
        self.sent = b""
        self.closed = False

    def sendall(self, data):
        self.sent += data

    def makefile(self, mode):
        reply = self.replies.pop(0)
        if reply is None:
            return TimingOut()
        return io.BytesIO(reply)

    def close(self):
        self.closed = True


class TimingOut(io.BytesIO):
    def readline(self, *args):
        raise TimeoutError("timed out")

    read = readline


def http_reply(body: bytes) -> bytes:
    return b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body)


def fake_connections(*sockets):
    """Returns a `connect` callable that hands out one connection per socket, and the sockets."""
    pending = list(sockets)

    def connect():
        conn = http.client.HTTPConnection("127.0.0.1", 8545)
        conn.sock = pending.pop(0)
        return conn

    return connect


class SlowHandshake(http.client.HTTPConnection):
    """A connection whose TCP handshake takes 50 ms."""

    def __init__(self, sock):
        super().__init__("127.0.0.1", 8545)
        self.fake = sock

    def connect(self):
        time.sleep(0.05)
        self.sock = self.fake


class CallRawTest(unittest.TestCase):
    def test_a_timeout_is_an_error_with_the_timeout_as_latency_and_the_next_call_reconnects(self):
        slow, fresh = FakeSocket(None), FakeSocket(http_reply(b'{"result":1}'))
        client = rpc.Client("http://127.0.0.1:8545", timeout_s=30, connect=fake_connections(slow, fresh))

        self.assertEqual(client.call_raw(b"{}"), (None, b"", 30000.0, "timeout"))
        self.assertTrue(slow.closed)

        status, body, latency_ms, error = client.call_raw(b"{}")
        self.assertEqual((status, body, error), (200, b'{"result":1}', None))
        self.assertGreaterEqual(latency_ms, 0)
        self.assertIn(b"POST / HTTP/1.1", fresh.sent)

    def test_a_dropped_keep_alive_connection_is_reopened_once(self):
        dropped = FakeSocket(http_reply(b"1"), b"")
        fresh = FakeSocket(http_reply(b"2"))
        client = rpc.Client("http://127.0.0.1:8545", connect=fake_connections(dropped, fresh))

        self.assertEqual(client.call_raw(b"{}").body, b"1")
        status, body, _, error = client.call_raw(b"{}")
        self.assertEqual((status, body, error), (200, b"2", None))
        self.assertTrue(dropped.closed)

    def test_a_fresh_connection_that_fails_is_an_error(self):
        client = rpc.Client("http://127.0.0.1:8545", connect=fake_connections(FakeSocket(b"")))
        status, body, _, error = client.call_raw(b"{}")
        self.assertEqual((status, body), (None, b""))
        self.assertEqual(error, "connection: Remote end closed connection without response")

    def test_the_handshake_of_a_new_connection_is_not_timed(self):
        client = rpc.Client("http://127.0.0.1:8545", connect=lambda: SlowHandshake(FakeSocket(http_reply(b"1"))))
        response = client.call_raw(b"{}")
        self.assertEqual(response.body, b"1")
        self.assertLess(response.latency_ms, 50)


class CallTest(unittest.TestCase):
    def test_returns_the_result(self):
        reply = http_reply(b'{"jsonrpc":"2.0","id":1,"result":"0x10"}')
        client = rpc.Client("http://127.0.0.1:8545", connect=fake_connections(FakeSocket(reply)))
        self.assertEqual(client.call("eth_blockNumber", []), "0x10")

    def test_raises_the_json_rpc_error(self):
        error = {"code": -32602, "message": "invalid params"}
        reply = http_reply(json.dumps({"jsonrpc": "2.0", "id": 1, "error": error}).encode())
        client = rpc.Client("http://127.0.0.1:8545", connect=fake_connections(FakeSocket(reply)))
        with self.assertRaises(rpc.RpcError) as raised:
            client.call("eth_getLogs", [{}])
        self.assertEqual(str(raised.exception), 'eth_getLogs: {"code": -32602, "message": "invalid params"}')

    def test_a_reply_without_a_result_is_an_error(self):
        reply = http_reply(b'{"jsonrpc":"2.0","id":1}')
        client = rpc.Client("http://127.0.0.1:8545", connect=fake_connections(FakeSocket(reply)))
        with self.assertRaises(rpc.RpcError) as raised:
            client.call("eth_blockNumber", [])
        self.assertEqual(str(raised.exception), """eth_blockNumber: HTTP 200: b'{"jsonrpc":"2.0","id":1}'""")
