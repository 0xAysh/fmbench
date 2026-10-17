"""JSON-RPC over one persistent HTTP connection, with a per-request timeout.

`call_raw` is the timed path: it measures from the request write to the last body byte and never
parses the body. `call` is for setup work (sampling, readiness checks), where parsing is fine.
"""

from __future__ import annotations

import http.client
import json
import time
import urllib.parse
from collections.abc import Callable
from typing import Any, NamedTuple

# The default per-request timeout (roadmap §1.8): a timeout is a recorded error, never a hang.
DEFAULT_TIMEOUT_S = 1800

TIMEOUT = "timeout"


class Response(NamedTuple):
    """One raw exchange. `status` is `None` when no response arrived; `error` is `None`, `"timeout"`
    or a connection error. A JSON-RPC error inside a 200 response is not an `error` here: the body
    is parsed after the timed region."""

    status: int | None
    body: bytes
    latency_ms: float
    error: str | None


class RpcError(Exception):
    """A JSON-RPC error response, or a transport error, from `Client.call`."""


class Client:
    """A JSON-RPC client that keeps one HTTP/1.1 keep-alive connection to `url`.

    `connect` builds a new connection; tests pass one that wraps a fake socket.
    """

    def __init__(
        self,
        url: str,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        connect: Callable[[], http.client.HTTPConnection] | None = None,
    ) -> None:
        parts = urllib.parse.urlsplit(url)
        self.path = parts.path or "/"
        self.timeout_s = timeout_s
        host, port = parts.hostname or "127.0.0.1", parts.port or 80
        self._connect = connect or (lambda: http.client.HTTPConnection(host, port, timeout=timeout_s))
        self._conn: http.client.HTTPConnection | None = None
        self._next_id = 0

    def call_raw(self, body: bytes) -> Response:
        """Sends `body` and reads the whole response without parsing it.

        A timeout closes the connection and returns `error="timeout"` with the timeout as latency;
        the next call reconnects. A dropped keep-alive connection is reopened once and the request
        sent again (log queries are idempotent); only the second attempt is timed. The TCP
        handshake of a new connection is not timed either.
        """
        retried = False
        while True:
            reused = self._conn is not None
            if self._conn is None:
                self._conn = self._connect()
            # A reused connection is timed from here, a new one from the end of its handshake.
            started = time.perf_counter()
            try:
                if self._conn.sock is None:
                    self._conn.connect()
                    started = time.perf_counter()
                self._conn.request(
                    "POST", self.path, body=body, headers={"Content-Type": "application/json"}
                )
                response = self._conn.getresponse()
                data = response.read()
            except TimeoutError:
                self.close()
                return Response(None, b"", self.timeout_s * 1000.0, TIMEOUT)
            except (ConnectionError, http.client.HTTPException, OSError) as error:
                self.close()
                if reused and not retried:
                    retried = True
                    continue
                return Response(None, b"", (time.perf_counter() - started) * 1000.0, f"connection: {error}")
            latency_ms = (time.perf_counter() - started) * 1000.0
            if response.will_close:
                self.close()
            return Response(response.status, data, latency_ms, None)

    def request_body(self, method: str, params: list[Any]) -> bytes:
        """Encodes one JSON-RPC request."""
        self._next_id += 1
        return json.dumps(
            {"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params},
            separators=(",", ":"),
        ).encode()

    def call(self, method: str, params: list[Any]) -> Any:
        """Calls `method` and returns its result. Raises `RpcError` on a JSON-RPC error, a
        non-200 status, a timeout or a connection error."""
        response = self.call_raw(self.request_body(method, params))
        if response.error is not None:
            raise RpcError(f"{method}: {response.error}")
        try:
            reply = json.loads(response.body)
        except ValueError as error:
            raise RpcError(f"{method}: HTTP {response.status}: {response.body[:200]!r}") from error
        if isinstance(reply, dict) and "error" in reply:
            raise RpcError(f"{method}: {json.dumps(reply['error'])}")
        if response.status != 200 or not isinstance(reply, dict) or "result" not in reply:
            raise RpcError(f"{method}: HTTP {response.status}: {response.body[:200]!r}")
        return reply["result"]

    def close(self) -> None:
        """Closes the connection; the next call opens a new one."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
