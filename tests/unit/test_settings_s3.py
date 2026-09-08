from datetime import date


def test_s3_settings_have_local_defaults():
    from config.settings import Settings

    settings = Settings(DATABASE_URL="postgresql+asyncpg://x:y@localhost/z")

    assert settings.S3_ENDPOINT_URL == "http://localhost:8333"
    assert settings.S3_BUCKET == "pmt-bucket"
    assert settings.S3_PRICES_KEY == "config/prices.json"
    assert settings.S3_ROLLUP_PREFIX == "rollups/"
    assert settings.PRICES_CACHE_TTL_S == 60


def test_rollup_key_joins_prefix_and_iso_date():
    from config.settings import Settings

    settings = Settings(DATABASE_URL="postgresql+asyncpg://x:y@localhost/z")

    assert settings.rollup_key(date(2026, 9, 7)) == "rollups/2026-09-07.json"


def test_rollup_key_respects_a_custom_prefix():
    from config.settings import Settings

    settings = Settings(
        DATABASE_URL="postgresql+asyncpg://x:y@localhost/z",
        S3_ROLLUP_PREFIX="daily/",
    )

    assert settings.rollup_key(date(2026, 9, 7)) == "daily/2026-09-07.json"
