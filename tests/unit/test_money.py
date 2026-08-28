import pytest

from money import format_cents


@pytest.mark.parametrize(
    ("cents", "expected"),
    [
        (0, "0.00"),
        (5, "0.05"),
        (50, "0.50"),
        (450, "4.50"),
        (1000, "10.00"),
        (1350, "13.50"),
        (123456, "1234.56"),
        (-1350, "-13.50"),
        (-5, "-0.05"),
    ],
)
def test_format_cents(cents: int, expected: str):
    assert format_cents(cents) == expected
