"""Offline audit of immutable SZSE receipts; never grants market admission."""
import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError(f"nonfinite number: {value}")


def decode(raw):
    return json.loads(raw, parse_float=Decimal, parse_constant=reject_constant,
                      object_pairs_hook=unique_object)


def cents(value):
    # Work from the original decimal tuple; no float or context rounding.
    if type(value) not in (int, Decimal):
        raise ValueError("amount must be a JSON number")
    number = Decimal(value)
    sign, digits, exponent = number.as_tuple()
    if sign or len(digits) > 40 or abs(exponent) > 40:
        raise ValueError("amount outside research numeric bounds")
    coefficient = int("".join(map(str, digits)))
    shift = exponent + 2
    if shift >= 0:
        return coefficient * 10 ** shift
    divisor = 10 ** (-shift)
    if coefficient % divisor:
        raise ValueError("sub-cent amount")
    return coefficient // divisor


def audit(raw):
    data = decode(raw)
    if data["code"] != "0" or data["data"]["code"] != "002256" or data["data"]["name"] != "兆新股份":
        raise ValueError("instrument identity mismatch")
    quote = data["data"]
    total = 0
    volume = 0
    previous = ""
    count = 0
    for row in quote["picupdata"]:
        if len(row) != 7 or not isinstance(row[0], str):
            raise ValueError("unexpected row schema")
        label = row[0]
        if len(label) != 5 or label[2] != ":" or not (label[:2] + label[3:]).isdigit():
            raise ValueError("invalid minute label")
        if not (0 <= int(label[:2]) < 24 and 0 <= int(label[3:]) < 60) or label <= previous:
            raise ValueError("invalid or nonincreasing minute label")
        previous = label
        amount = cents(row[6])
        if type(row[5]) is not int or row[5] < 0:
            raise ValueError("invalid volume")
        if "09:30" <= label <= "11:30" or "13:01" <= label <= "15:00":
            count += 1
            total += amount
            volume += row[5]
        elif not "15:06" <= label <= "15:30":
            raise ValueError("unexpected session label")
    if count == 0 or type(quote["volume"]) is not int or quote["volume"] < 0:
        raise ValueError("missing regular rows or invalid aggregate volume")
    aggregate = cents(quote["amount"])
    return {"raw_sha256": hashlib.sha256(raw).hexdigest(),
            "regular_rows": count, "regular_amount_cents": str(total),
            "aggregate_amount_cents": str(aggregate),
            "regular_volume_lots": volume, "aggregate_volume_lots": quote["volume"],
            "totals_equal": total == aggregate and volume == quote["volume"],
            "admitted": False, "completion_proven": False,
            "numeric_path": "original JSON decimal tokens to integer cents"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    receipt = decode(args.receipt.read_bytes())
    name = receipt["raw_file"]
    if Path(name).name != name or not name.startswith("response-"):
        raise ValueError("unsafe raw filename")
    raw = (args.receipt.parent / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != receipt["raw_sha256"]:
        raise ValueError("raw receipt SHA mismatch")
    print(json.dumps(audit(raw), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
