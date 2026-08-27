# db/__init__.py.py

from db.postgres.session import get_db

__all__ = ["get_db"]
