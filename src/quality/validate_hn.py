"""
SciPulse — Validation Great Expectations des items Hacker News.

Usage :
    python -m src.quality.validate_hn
"""

import argparse
import sys

import great_expectations.expectations as gxe
import pandas as pd
from elasticsearch import Elasticsearch

from src.quality.extract_for_validation import HN_COLUMNS, extract_hn
from src.quality.gx_runner import print_report, run_suite
from src.utils.config import ES_HOST, IDX_HN

HN_TYPES = ["story", "comment", "job", "poll", "pollopt"]


def build_expectations() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="hn_id"),
        gxe.ExpectColumnValuesToBeUnique(column="hn_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="type"),
        gxe.ExpectColumnValuesToBeInSet(column="type", value_set=HN_TYPES),
        gxe.ExpectColumnValuesToBeBetween(column="score", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="descendants", min_value=0),
        # time : ISO-8601, 2006 (création de HN) → aujourd'hui
        gxe.ExpectColumnValuesToMatchRegex(
            column="time", regex=r"^(200[6-9]|20[1-3]\d)-\d{2}-\d{2}", mostly=0.99
        ),
        gxe.ExpectTableColumnsToMatchSet(column_set=HN_COLUMNS, exact_match=True),
    ]


def validate_hn_data(df: pd.DataFrame):
    return run_suite("hn_items", df, build_expectations())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SciPulse — validation HN (GX)")
    parser.add_argument("--index", default=IDX_HN)
    args = parser.parse_args(argv)

    df = extract_hn(Elasticsearch(ES_HOST), index=args.index)
    if df.empty:
        print("❌ Aucun item HN à valider — lancer le poller d'abord")
        return 1
    result = validate_hn_data(df)
    print_report(result)
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
