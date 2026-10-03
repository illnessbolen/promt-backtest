"""Reconnect / resubscribe / silent-socket behaviour of the shared WebSocket client against a local server."""

import asyncio
import json

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from bosona.live.wsconn import WsClient


async def _run_client(client: WsClient, got: list, until, timeout: float = 10.0) -> None:
    async def on_open(c: WsClient) -> None:
        await c.send({"op": "subscribe"})

    def on_message(raw: str, recv: float) -> None:
        got.append(raw)
        if raw.startswith("data"):
            c.mark_data()

    c = client
    task = asyncio.create_task(client.run(on_open, on_message))
    try:
        async with asyncio.timeout(timeout):
            while not until():
                await asyncio.sleep(0.02)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_reconnects_and_resubscribes_after_server_close():
    subscribes = []

    async def handler(ws):
        try:
            subscribes.append(json.loads(await ws.recv()))
            await ws.send(f"data {len(subscribes)}")
            await ws.close()  # server drops every session after one message
        except ConnectionClosed:
            pass

    async def main():
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            client = WsClient("test", f"ws://127.0.0.1:{port}", idle_timeout_s=5, backoff_base_s=0.05)
            got = []
            await _run_client(client, got, lambda: len(got) >= 3)
            return client, got

    client, got = asyncio.run(main())
    assert got[:3] == ["data 1", "data 2", "data 3"]
    assert len(subscribes) >= 3 and all(s == {"op": "subscribe"} for s in subscribes)
    assert client.connects >= 3


def test_socket_without_data_is_recycled():
    sessions = []

    async def handler(ws):
        sessions.append(1)
        try:
            await ws.recv()
            while True:  # answers like a removed topic: only keep-alives, never data
                await ws.send("PONG")
                await asyncio.sleep(0.05)
        except ConnectionClosed:
            pass

    async def main():
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            client = WsClient("test", f"ws://127.0.0.1:{port}", idle_timeout_s=5, data_timeout_s=0.3, backoff_base_s=0.05)
            await _run_client(client, [], lambda: len(sessions) >= 3)
            return client

    client = asyncio.run(main())
    assert client.connects >= 2 and "no data" in client.last_error and client.failures >= 1
