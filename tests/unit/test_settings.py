import os

from config.settings import Settings


def test_settings_construct_with_no_environment_at_all(monkeypatch):
    """
    DATABASE_URL was the only required field. With the database layer gone the
    template must be runnable with zero configuration — a new user should be
    able to `python main.py` against a default local Redis.
    """
    for key in list(os.environ):
        if key.startswith(
            ("DATABASE", "REDIS", "STREAM", "CONSUMER", "HEALTH", "STATE")
        ):
            monkeypatch.delenv(key, raising=False)

    settings = Settings(_env_file=None)

    assert settings.REDIS_STREAM_URL == "redis://localhost:6379/1"
    assert settings.HEALTH_PORT == 8000
    assert settings.STATE_TTL_SECONDS == 3600
    assert not hasattr(settings, "DATABASE_URL")
    assert not hasattr(settings, "SERVICE_PORT")
