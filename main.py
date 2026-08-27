# pmt/main.py

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.routes import health, info, orders
from config.logging import configure_logging, get_logger
from config.request_logger import RequestLoggingMiddleware
from config.tracing import init_tracing
from db.postgres.session import close_db
from messaging.consumer import RedisConsumer
from messaging.producer.redis_producer import close_producer

configure_logging()

logger = get_logger(__name__)

# Global consumer instance
consumer = None
consumer_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Start the Redis consumer alongside the API.

    The schema is NOT created here — run `alembic upgrade head` (the compose
    entrypoint and `make migrate` both do).
    """
    global consumer, consumer_task

    logger.info("Starting application")

    consumer = RedisConsumer()
    consumer_task = asyncio.create_task(consumer.start())
    consumer_task.add_done_callback(
        lambda t: (
            logger.error("Consumer task died", error=str(t.exception()))
            if not t.cancelled() and t.exception()
            else None
        )
    )
    logger.info("Redis Stream consumer started")

    yield

    logger.info("Shutting down application")

    if consumer:
        await consumer.stop()
    if consumer_task:
        try:
            # Let the in-flight message finish before giving up on it.
            await asyncio.wait_for(consumer_task, timeout=10)
        except TimeoutError:
            logger.warning("Consumer did not stop in time; cancelling")
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # Whatever killed the consumer task must not skip the cleanup
            # below (producer/db close) — that's a resource leak.
            logger.error("Consumer task ended with error", error=str(e))
    if consumer:
        await consumer.close()
    logger.info("Redis Stream consumer stopped")

    await close_producer()
    await close_db()
    logger.info("Connections closed")


app = FastAPI(
    title="Python Microservice Template Service",
    version="1.0.0",
    description="Python Microservice Template API",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(RequestLoggingMiddleware)

# OTEL tracing
init_tracing(app)

# -----------------------------------------------------------
# Middleware
# -----------------------------------------------------------
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=settings.CORS_ALLOW_ORIGINS,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# -----------------------------------------------------------
# Routers
# -----------------------------------------------------------
app.include_router(health.router)
app.include_router(info.router)
app.include_router(orders.router)


# -----------------------------------------------------------
# Root Endpoint
# -----------------------------------------------------------
@app.get("/", tags=["root"])
async def root():
    return {"message": "Python Microservice Template API is running"}


# -----------------------------------------------------------
# Main function (run with: python -m pmt.main)
# -----------------------------------------------------------
def main():
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8099,
        reload=True,
    )


if __name__ == "__main__":
    main()
