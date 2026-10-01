import json

import pytest

from pipelines.transforms import InvalidRecord, parse_csv_line, parse_json_message, validate


def test_parse_and_validate_csv():
    rec = validate(parse_csv_line("o1,c9,ne,19.999,usd,2025-05-01T10:00:00Z"))
    assert rec["amount"] == 20.0 and rec["currency"] == "USD" and rec["region"] == "NE"


@pytest.mark.parametrize("line", [
    "o1,c9,ne,abc,usd,2025-05-01T10:00:00",
    "o1,c9,ne,-1,usd,2025-05-01T10:00:00",
    "o1,c9,ne,5,xyz,2025-05-01T10:00:00",
    ",c9,ne,5,usd,2025-05-01T10:00:00",
    "o1,c9,ne,5,usd,not-a-date",
])
def test_invalid_rows(line):
    with pytest.raises(InvalidRecord):
        validate(parse_csv_line(line))


def test_wrong_column_count():
    with pytest.raises(InvalidRecord):
        parse_csv_line("a,b,c")


def test_json_message():
    msg = json.dumps({"order_id": "1", "amount": 3, "currency": "EUR", "order_ts": "2025-01-01T00:00:00"}).encode()
    assert validate(parse_json_message(msg))["currency"] == "EUR"
    with pytest.raises(InvalidRecord):
        parse_json_message(b"{bad")
