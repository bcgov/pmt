# tests/unit/test_main.py

import asyncio

from httpx import ASGITransport, AsyncClient

import main


async def noop():
    pass


class FakeConsumer:
    """
    Mirrors RedisConsumer's lifecycle without touching Redis.

    start() blocks until stop() is called, like the real consumer loop
    blocks until self.running goes False.
    """

    def __init__(self):
        self.started = asyncio.Event()
        self._run_forever = asyncio.Event()
        self.stopped = False
        self.closed = False

    async def start(self):
        self.started.set()
        await self._run_forever.wait()

    async def stop(self):
        self.stopped = True
        self._run_forever.set()

    async def close(self):
        self.closed = True


async def test_root_endpoint_returns_ok_message():
    transport = ASGITransport(app=main.app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert response.json() == {"message": "Python Microservice Template API is running"}


def test_main_runs_uvicorn_on_the_expected_host_and_port(monkeypatch):
    import uvicorn

    calls = []
    monkeypatch.setattr(
        uvicorn, "run", lambda app_path, **kwargs: calls.append((app_path, kwargs))
    )

    main.main()

    assert calls == [("main:app", {"host": "0.0.0.0", "port": 8099, "reload": True})]


async def test_lifespan_starts_and_cleanly_stops_the_consumer(monkeypatch):
    fake_consumer = FakeConsumer()
    producer_closed, db_closed = [], []
    monkeypatch.setattr(main, "RedisConsumer", lambda: fake_consumer)
    monkeypatch.setattr(
        main, "close_producer", lambda: producer_closed.append(True) or noop()
    )
    monkeypatch.setattr(main, "close_db", lambda: db_closed.append(True) or noop())

    async with main.lifespan(main.app):
        await fake_consumer.started.wait()

    assert fake_consumer.stopped is True
    assert fake_consumer.closed is True
    assert producer_closed == [True]
    assert db_closed == [True]


async def test_lifespan_cancels_a_consumer_task_that_wont_stop_in_time(monkeypatch):
    fake_consumer = FakeConsumer()

    async def stop_without_unblocking_start():
        fake_consumer.stopped = True

    fake_consumer.stop = stop_without_unblocking_start
    monkeypatch.setattr(main, "RedisConsumer", lambda: fake_consumer)
    monkeypatch.setattr(main, "close_producer", noop)
    monkeypatch.setattr(main, "close_db", noop)

    async def fake_wait_for(aw, timeout=None):
        raise TimeoutError

    monkeypatch.setattr(main.asyncio, "wait_for", fake_wait_for)

    async with main.lifespan(main.app):
        await fake_consumer.started.wait()

    assert fake_consumer.closed is True


async def test_lifespan_swallows_cancelled_error_while_waiting_for_the_task(
    monkeypatch,
):
    fake_consumer = FakeConsumer()
    monkeypatch.setattr(main, "RedisConsumer", lambda: fake_consumer)
    monkeypatch.setattr(main, "close_producer", noop)
    monkeypatch.setattr(main, "close_db", noop)

    async def fake_wait_for(aw, timeout=None):
        raise asyncio.CancelledError

    monkeypatch.setattr(main.asyncio, "wait_for", fake_wait_for)

    async with main.lifespan(main.app):
        await fake_consumer.started.wait()

    assert fake_consumer.closed is True


async def test_lifespan_logs_and_continues_when_the_consumer_task_errors(monkeypatch):
    class ExplodingConsumer:
        def __init__(self):
            self.closed = False

        async def start(self):
            raise RuntimeError("boom")

        async def stop(self):
            pass

        async def close(self):
            self.closed = True

    consumer = ExplodingConsumer()
    monkeypatch.setattr(main, "RedisConsumer", lambda: consumer)
    monkeypatch.setattr(main, "close_producer", noop)
    monkeypatch.setattr(main, "close_db", noop)

    async with main.lifespan(main.app):
        await asyncio.sleep(0)  # let the consumer task run and raise

    assert consumer.closed is True
