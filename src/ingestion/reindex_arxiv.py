"""
SciPulse — Réindexation arxiv-papers-raw -> arxiv-papers (mapping optimisé).

Transformations : auteurs nested (nom + affiliation), authors_flat,
date_published (première version), doi, catégories.

Usage :
    python -m src.ingestion.reindex_arxiv
    python -m src.ingestion.reindex_arxiv --recreate
"""

import argparse
import logging
import re
import sys
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Iterator

from elasticsearch import Elasticsearch, helpers

from src.utils.config import ES_HOST, IDX_ARXIV, IDX_ARXIV_RAW
from src.utils.mappings import create_indices

logger = logging.getLogger(__name__)


def parse_version_date(value: str | None) -> str | None:
    """'Mon, 2 Apr 2007 19:18:42 GMT' -> '2007-04-02'."""
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).date().isoformat()
    except (TypeError, ValueError):
        return None


def build_authors(src: dict) -> tuple[list[dict], list[str]]:
    """Retourne (authors nested, authors_flat) depuis `authors_parsed` ou `authors`."""
    nested: list[dict] = []
    parsed = src.get("authors_parsed")
    if parsed:
        for entry in parsed:
            last, first = (entry + ["", ""])[:2]
            name = " ".join(p for p in (first.strip(), last.strip()) if p)
            affiliation = entry[3].strip() if len(entry) > 3 and entry[3] else None
            if name:
                nested.append({"name": name, **({"affiliation": affiliation} if affiliation else {})})
    elif isinstance(src.get("authors"), str):
        for part in re.split(r",\s*|\s+and\s+", src["authors"]):
            part = part.strip()
            if part:
                nested.append({"name": part})
    return nested, [a["name"] for a in nested]


def transform(src: dict) -> dict:
    authors, flat = build_authors(src)
    versions = src.get("versions") or []
    first_created = versions[0].get("created") if versions and isinstance(versions[0], dict) else None
    date_published = parse_version_date(first_created) or _date_only(src.get("date_updated"))
    cats = src.get("categories") or []
    if isinstance(cats, str):
        cats = cats.split()
    return {
        "arxiv_id": src["arxiv_id"],
        "title": (src.get("title") or "").strip(),
        "abstract": (src.get("abstract") or "").strip(),
        "authors": authors,
        "authors_flat": flat,
        "categories": cats,
        "primary_category": src.get("primary_category") or (cats[0] if cats else None),
        "date_published": date_published,
        "date_updated": _date_only(src.get("date_updated")),
        "doi": src.get("doi"),
    }


def _date_only(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return value[:10]


def actions(es: Elasticsearch, source: str, dest: str) -> Iterator[dict]:
    for hit in helpers.scan(es, index=source, query={"query": {"match_all": {}}}, size=1000):
        src = hit["_source"]
        if not src.get("arxiv_id"):
            continue
        yield {"_index": dest, "_id": src["arxiv_id"], "_source": transform(src)}


def reindex(es: Elasticsearch, source: str = IDX_ARXIV_RAW, dest: str = IDX_ARXIV):
    ok, errors = helpers.bulk(es, actions(es, source, dest), chunk_size=1000, raise_on_error=False)
    es.indices.refresh(index=dest)
    logger.info("✅ %d documents réindexés %s -> %s (%d erreurs)", ok, source, dest, len(errors))
    return ok, errors


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="SciPulse — réindexation arxiv")
    parser.add_argument("--recreate", action="store_true", help="Recrée l'index arxiv-papers avant réindexation")
    args = parser.parse_args(argv)

    es = Elasticsearch(ES_HOST)
    create_indices(es, recreate=args.recreate, names=[IDX_ARXIV])  # ne touche pas à hn-items
    _, errors = reindex(es)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
