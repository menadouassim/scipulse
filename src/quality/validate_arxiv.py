"""
SciPulse — Validation Great Expectations des articles ArXiv.

Usage :
    python -m src.quality.validate_arxiv                  # index arxiv-papers
    python -m src.quality.validate_arxiv --enriched       # + champs d'enrichissement
    python -m src.quality.validate_arxiv --index arxiv-papers-raw
"""

import argparse
import sys

import great_expectations.expectations as gxe
import pandas as pd
from elasticsearch import Elasticsearch

from src.quality.extract_for_validation import (
    ARXIV_COLUMNS,
    ARXIV_ENRICHED_COLUMNS,
    extract_arxiv,
)
from src.quality.gx_runner import print_report, run_suite
from src.utils.config import ES_HOST, IDX_ARXIV

KNOWN_CS_CATEGORIES = [
    "cs.AI", "cs.AR", "cs.CC", "cs.CE", "cs.CG", "cs.CL", "cs.CR", "cs.CV", "cs.CY",
    "cs.DB", "cs.DC", "cs.DL", "cs.DM", "cs.DS", "cs.ET", "cs.FL", "cs.GL", "cs.GR",
    "cs.GT", "cs.HC", "cs.IR", "cs.IT", "cs.LG", "cs.LO", "cs.MA", "cs.MM", "cs.MS",
    "cs.NA", "cs.NE", "cs.NI", "cs.OH", "cs.OS", "cs.PF", "cs.PL", "cs.RO", "cs.SC",
    "cs.SD", "cs.SE", "cs.SI", "cs.SY", "stat.ML", "stat.ME", "stat.TH", "stat.AP",
    "stat.CO", "math.IT", "math.OC", "math.ST", "eess.AS", "eess.IV", "eess.SP",
    "eess.SY", "q-bio.NC", "physics.soc-ph",
]


def build_expectations(df_len: int, enriched: bool = False) -> list:
    exps = [
        # Complétude
        gxe.ExpectColumnValuesToNotBeNull(column="arxiv_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="title"),
        gxe.ExpectColumnValuesToNotBeNull(column="abstract", mostly=0.99),
        gxe.ExpectColumnValuesToNotBeNull(column="primary_category"),
        # Unicité
        gxe.ExpectColumnValuesToBeUnique(column="arxiv_id"),
        # Validité
        gxe.ExpectColumnValueLengthsToBeBetween(column="abstract", min_value=50, mostly=0.98),
        gxe.ExpectColumnValueLengthsToBeBetween(column="title", min_value=10, mostly=0.99),
        gxe.ExpectColumnValuesToBeInSet(
            column="primary_category", value_set=KNOWN_CS_CATEGORIES, mostly=0.95
        ),
        gxe.ExpectColumnValuesToMatchRegex(
            column="date_published", regex=r"^\d{4}-\d{2}-\d{2}", mostly=0.99
        ),
        # Plage réaliste 1991–2025
        gxe.ExpectColumnValuesToMatchRegex(
            column="date_published", regex=r"^(199\d|20[01]\d|202[0-5])-", mostly=0.99
        ),
        # Volume & schéma
        gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=5_000_000),
        gxe.ExpectTableColumnsToMatchSet(
            column_set=ARXIV_ENRICHED_COLUMNS if enriched else ARXIV_COLUMNS, exact_match=True
        ),
    ]
    if enriched:
        exps += [
            gxe.ExpectColumnValuesToBeBetween(column="hn_score", min_value=0, mostly=1.0),
            gxe.ExpectColumnValuesToBeBetween(column="hn_comments_count", min_value=0, mostly=1.0),
        ]
    return exps


def validate_arxiv_data(df: pd.DataFrame, enriched: bool = False):
    name = "arxiv_enriched" if enriched else "arxiv_raw"
    return run_suite(name, df, build_expectations(len(df), enriched))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SciPulse — validation ArXiv (GX)")
    parser.add_argument("--index", default=IDX_ARXIV)
    parser.add_argument("--enriched", action="store_true")
    parser.add_argument("--max-docs", type=int, default=10_000)
    args = parser.parse_args(argv)

    es = Elasticsearch(ES_HOST)
    cols = ARXIV_ENRICHED_COLUMNS if args.enriched else ARXIV_COLUMNS
    df = extract_arxiv(es, index=args.index, max_docs=args.max_docs, columns=cols)
    if df.empty:
        print("❌ Aucun document à valider")
        return 1
    result = validate_arxiv_data(df, enriched=args.enriched)
    print_report(result)
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
