"""
SciPulse — Nettoyage des données ArXiv
Guidé par les résultats des validations Great Expectations.

Ce script :
  1. Extrait les données depuis Elasticsearch
  2. Applique les corrections
  3. Affiche un rapport avant/après
  4. Réindexe les données corrigées

Usage :
    python -m src.quality.clean_arxiv
    python -m src.quality.clean_arxiv --dry-run     # Simuler sans réindexer
"""

import argparse
import re
import sys
import time

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from src.utils.config import ES_HOST, IDX_ARXIV

INDEX = IDX_ARXIV
BATCH_SIZE = 500
MIN_ABSTRACT_LEN = 50
_MULTI_SPACE = re.compile(r"\s{2,}")


def load_all_articles(es: Elasticsearch, index: str = INDEX) -> pd.DataFrame:
    """Charge tous les articles depuis Elasticsearch (_id + _source)."""
    rows = []
    for hit in helpers.scan(es, index=index, query={"query": {"match_all": {}}}, size=1000):
        rows.append({"_id": hit["_id"], **hit["_source"]})
    return pd.DataFrame(rows)


def diagnose(df: pd.DataFrame, known_categories: set[str] | None = None) -> dict:
    """Analyse les problèmes de qualité et retourne un rapport."""
    from src.quality.validate_arxiv import KNOWN_CS_CATEGORIES

    known = known_categories or set(KNOWN_CS_CATEGORIES)
    abstract = df["abstract"] if "abstract" in df else pd.Series(dtype=object)
    title = df["title"] if "title" in df else pd.Series(dtype=object)
    id_col = "arxiv_id" if "arxiv_id" in df else "_id"

    return {
        "null_abstracts": int(abstract.isna().sum()),
        "short_abstracts": int((abstract.dropna().str.len() < MIN_ABSTRACT_LEN).sum()),
        "null_titles": int(title.isna().sum()),
        "empty_titles": int((title.dropna().str.strip() == "").sum()),
        "duplicates": int(df.duplicated(subset=[id_col]).sum()),
        "unknown_categories": int(
            (~df["primary_category"].dropna().isin(known)).sum() if "primary_category" in df else 0
        ),
        "multi_space_titles": int(title.dropna().str.contains(_MULTI_SPACE).sum()),
        "null_primary_category": int(df["primary_category"].isna().sum()) if "primary_category" in df else 0,
    }


def print_diagnosis(report: dict, label: str):
    print(f"\n  {'─' * 50}")
    print(f"  {label}")
    print(f"  {'─' * 50}")
    print(f"  Abstracts nulls          : {report['null_abstracts']:>6,}")
    print(f"  Abstracts < 50 car.      : {report['short_abstracts']:>6,}")
    print(f"  Titres nulls/vides       : {report['null_titles'] + report['empty_titles']:>6,}")
    print(f"  Doublons (arxiv_id)      : {report['duplicates']:>6,}")
    print(f"  Catégories inconnues     : {report['unknown_categories']:>6,}")
    print(f"  Titres avec multi-espaces: {report['multi_space_titles']:>6,}")
    print(f"  Catégorie primaire nulle : {report['null_primary_category']:>6,}")


def _squash(value):
    return _MULTI_SPACE.sub(" ", value).strip() if isinstance(value, str) else value


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Applique les corrections de qualité. Retourne un nouveau DataFrame."""
    df = df.copy()

    # 1. Doublons : garder la dernière date_updated
    if "date_updated" in df:
        df = df.sort_values("date_updated", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["arxiv_id"], keep="first")

    # 2. Abstracts : retours à la ligne -> espaces, puis normalisation ; les trop courts sont signalés
    df["abstract"] = df["abstract"].map(lambda v: _squash(v.replace("\n", " ")) if isinstance(v, str) else v)
    df["abstract_too_short"] = df["abstract"].map(
        lambda v: not isinstance(v, str) or len(v) < MIN_ABSTRACT_LEN
    )

    # 3. Titres
    df["title"] = df["title"].map(lambda v: _squash(v.replace("\n", " ")) if isinstance(v, str) else v)
    df = df[df["title"].notna() & (df["title"] != "")]

    # 4. Auteurs : espaces superflus
    if "authors_flat" in df:
        df["authors_flat"] = df["authors_flat"].map(
            lambda names: [_squash(n) for n in names] if isinstance(names, list) else names
        )

    # 5. primary_category manquante -> première catégorie
    if "categories" in df:
        missing = df["primary_category"].isna() & df["categories"].map(
            lambda c: isinstance(c, list) and len(c) > 0
        )
        df.loc[missing, "primary_category"] = df.loc[missing, "categories"].map(lambda c: c[0])

    print(f"  ✅ Nettoyage terminé — {len(df):,} articles restants")
    return df.reset_index(drop=True)


def _clean_value(v):
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


def reindex_cleaned(es: Elasticsearch, df: pd.DataFrame, index: str = INDEX):
    """Réindexe (partial update) les données nettoyées et supprime les doublons éliminés."""
    print(f"\n🔄 Réindexation de {len(df):,} articles nettoyés...")
    keep_ids = set(df["arxiv_id"])

    def generate_actions():
        for rec in df.to_dict(orient="records"):
            rec.pop("_id", None)
            doc = {k: _clean_value(v) for k, v in rec.items() if _clean_value(v) is not None}
            yield {"_op_type": "index", "_index": index, "_id": doc["arxiv_id"], "_source": doc}

    start = time.time()
    success, errors = helpers.bulk(es, generate_actions(), chunk_size=BATCH_SIZE, raise_on_error=False)
    es.indices.refresh(index=index)
    # Les _id absents du DataFrame nettoyé (ex. doublons sous un autre _id) sont supprimés
    stale = [
        {"_op_type": "delete", "_index": index, "_id": h["_id"]}
        for h in helpers.scan(es, index=index, query={"query": {"match_all": {}}, "_source": False})
        if h["_id"] not in keep_ids
    ]
    if stale:
        helpers.bulk(es, stale, raise_on_error=False)
        es.indices.refresh(index=index)
    print(f"  ✅ {success:,} indexés, {len(stale):,} supprimés, {len(errors)} erreurs ({time.time() - start:.1f}s)")
    return success, errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SciPulse — Nettoyage ArXiv")
    parser.add_argument("--dry-run", action="store_true", help="Simuler sans réindexer")
    parser.add_argument("--index", default=INDEX)
    args = parser.parse_args(argv)

    print("\n╔══════════════════════════════════════════════════════════╗")
    print("║  SciPulse — Nettoyage des données ArXiv                 ║")
    print("╚══════════════════════════════════════════════════════════╝")

    es = Elasticsearch(ES_HOST)

    print("\n📥 Chargement des données...")
    df = load_all_articles(es, args.index)
    if df.empty:
        print("❌ Index vide")
        return 1
    print(f"  {len(df):,} documents chargés")

    report_before = diagnose(df)
    print_diagnosis(report_before, "AVANT nettoyage")

    print("\n🧹 Nettoyage en cours...")
    cleaned = clean(df)

    report_after = diagnose(cleaned)
    print_diagnosis(report_after, "APRÈS nettoyage")

    print(f"\n  {'─' * 50}\n  AMÉLIORATION\n  {'─' * 50}")
    for key in report_before:
        before, after = report_before[key], report_after[key]
        if before - after > 0:
            print(f"  📉 {key:<28} {before:>6,} → {after:>6,} ({before - after:>+6,})")
        elif before > 0:
            print(f"  ── {key:<28} {before:>6,} → {after:>6,} (inchangé)")

    if args.dry_run:
        print("\n🔎 Mode dry-run : aucune écriture.")
        return 0

    _, errors = reindex_cleaned(es, cleaned, args.index)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
