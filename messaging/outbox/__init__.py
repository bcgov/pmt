# messaging/outbox/__init__.py

from .relay import OutboxRelay, close_relay, get_relay

__all__ = ["OutboxRelay", "get_relay", "close_relay"]
