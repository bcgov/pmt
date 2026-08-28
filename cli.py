# cli.py
"""
The producer side of the sample, as a one-shot command.

Publishing lives in a CLI rather than an HTTP endpoint because this template
has no HTTP surface: a worker consumes events, and something else produces
them. Run it twice with the same --ref to watch the consumer's idempotency
guard turn the second delivery into a logged no-op.

    python -m cli publish --ref demo-1 --item widget \\
                          --quantity 3 --unit-price-cents 450 --count 2
"""

import argparse
import asyncio
import sys

from opentelemetry import trace

from config.logging import configure_logging
from config.tracing import init_tracing
from messaging.models import EventEnvelope, OrderCreatedEvent
from messaging.producer.redis_producer import RedisProducer
from money import format_cents

configure_logging()
init_tracing()

tracer = trace.get_tracer(__name__)


def non_negative_int(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return value


def positive_int(raw: str) -> int:
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("must be one or greater")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    publish_parser = sub.add_parser("publish", help="Publish an OrderCreated event")
    publish_parser.add_argument("--ref", required=True, help="Order reference")
    publish_parser.add_argument("--item", default="widget")
    publish_parser.add_argument("--quantity", type=positive_int, default=1)
    publish_parser.add_argument(
        "--unit-price-cents",
        type=non_negative_int,
        default=450,
        help="Unit price in integer minor units: 450 means 4.50",
    )
    publish_parser.add_argument(
        "--count",
        type=positive_int,
        default=1,
        help="Publish the same event N times, to demonstrate idempotency",
    )
    return parser


async def publish(args: argparse.Namespace) -> list[str]:
    """Publish `count` copies of one OrderCreated event; return message ids."""
    producer = RedisProducer()
    message_ids: list[str] = []
    try:
        for _ in range(args.count):
            # The CLI process is the root of the distributed trace. Without
            # this span the publish still works, but its context has no parent
            # and the worker's spans start a trace that begins mid-pipeline.
            with tracer.start_as_current_span("cli.publish"):
                message_ids.append(
                    await producer.publish(
                        EventEnvelope.create(
                            event_type="OrderCreated",
                            payload=OrderCreatedEvent(
                                order_ref=args.ref,
                                item=args.item,
                                quantity=args.quantity,
                                unit_price_cents=args.unit_price_cents,
                            ),
                            correlation_id=args.ref,
                            source="cli",
                        )
                    )
                )
    finally:
        await producer.close()
    return message_ids


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    message_ids = asyncio.run(publish(args))

    total = args.quantity * args.unit_price_cents
    print(
        f"published {len(message_ids)} x OrderCreated "
        f"ref={args.ref} qty={args.quantity} "
        f"unit={format_cents(args.unit_price_cents)} "
        f"expected_total={format_cents(total)}"
    )
    for message_id in message_ids:
        print(f"  {message_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
