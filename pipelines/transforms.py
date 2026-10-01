"""Parsing and validation logic shared by the batch and streaming pipelines."""
import csv
import io
import json
from datetime import datetime

FIELDS = ["order_id", "customer_id", "region", "amount", "currency", "order_ts"]
VALID_CURRENCIES = {"USD", "EUR", "GBP", "INR"}


class InvalidRecord(Exception):
    pass


def parse_csv_line(line: str) -> dict:
    values = next(csv.reader(io.StringIO(line)))
    if len(values) != len(FIELDS):
        raise InvalidRecord(f"expected {len(FIELDS)} columns, got {len(values)}")
    return dict(zip(FIELDS, values))


def parse_json_message(data: bytes) -> dict:
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise InvalidRecord(str(e))


def validate(rec: dict) -> dict:
    if not rec.get("order_id"):
        raise InvalidRecord("missing order_id")
    try:
        amount = float(rec["amount"])
    except (KeyError, TypeError, ValueError):
        raise InvalidRecord("amount is not numeric")
    if amount < 0:
        raise InvalidRecord("negative amount")
    currency = str(rec.get("currency", "")).upper()
    if currency not in VALID_CURRENCIES:
        raise InvalidRecord(f"unsupported currency {currency}")
    try:
        ts = datetime.fromisoformat(str(rec["order_ts"]).replace("Z", "+00:00"))
    except (KeyError, ValueError):
        raise InvalidRecord("bad order_ts")
    return {
        "order_id": str(rec["order_id"]),
        "customer_id": str(rec.get("customer_id", "")),
        "region": str(rec.get("region", "UNKNOWN")).upper(),
        "amount": round(amount, 2),
        "currency": currency,
        "order_ts": ts.isoformat(),
    }
