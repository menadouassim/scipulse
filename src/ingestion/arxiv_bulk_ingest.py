"""
SciPulse — Ingestion ArXiv en Python (alternative à Logstash).

Lit le JSON-lines (fichier local ou bucket MinIO), applique les mêmes
transformations que le pipeline Logstash et indexe avec `helpers.bulk`.

Usage :
    python -m src.ingestion.arxiv_bulk_ingest data/arxiv-raw/arxiv-cs-subset-100k.json
    python -m src.ingestion.arxiv_bulk_ingest --minio          # lit s3://arxiv-raw/chunks/
    python -m src.ingestion.arxiv_bulk_ingest FILE --index arxiv-papers-raw --limit 5000
"""

import argparse
import json
import logging
import sys
import time
from typing import Iterable, Iterator

from elasticsearch import Elasticsearch, helpers

from src.utils.config import ES_HOST, IDX_ARXIV_RAW

logger = logging.getLogger(__name__)

CHUNK = 1_000


def transform_raw(record: dict) -> dict | None:
    """Reproduit les filtres Logstash : renommage, split des catégories, nettoyage."""
    arxiv_id = record.get("id") or record.get("arxiv_id")
    if not arxiv_id:
        return None
    doc = {k: v for k, v in record.items() if k not in ("id", "update_date")}
    doc["arxiv_id"] = arxiv_id
    if record.get("update_date"):
        doc["date_updated"] = record["update_date"]

    cats = doc.get("categories")
    if isinstance(cats, str):
        cats = cats.split()
    if cats:
        doc["categories"] = cats
        doc["primary_category"] = cats[0]

    for field in ("abstract", "title"):
        if isinstance(doc.get(field), str):
            doc[field] = " ".join(doc[field].split())
    return doc


def iter_lines_file(path: str) -> Iterator[str]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield line


def iter_lines_minio(prefix: str = "chunks/") -> Iterator[str]:
    from src.ingestion.minio_client import get_minio_client

    client = get_minio_client()
    for name in sorted(
        o.object_name for o in client.list_objects("arxiv-raw", prefix=prefix, recursive=True)
    ):
        resp = client.get_object("arxiv-raw", name)
        try:
            for line in resp.read().decode("utf-8").splitlines():
                if line.strip():
                    yield line
        finally:
            resp.close()
            resp.release_conn()


def generate_actions(lines: Iterable[str], index: str, limit: int | None = None) -> Iterator[dict]:
    for n, line in enumerate(lines):
        if limit is not None and n >= limit:
            return
        try:
            doc = transform_raw(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("Ligne JSON invalide ignorée (#%d)", n)
            continue
        if doc is None:
            continue
        yield {"_op_type": "index", "_index": index, "_id": doc["arxiv_id"], "_source": doc}


def ingest(es: Elasticsearch, lines: Iterable[str], index: str = IDX_ARXIV_RAW, limit: int | None = None):
    start = time.time()
    ok, errors = helpers.bulk(
        es,
        generate_actions(lines, index, limit),
        chunk_size=CHUNK,
        raise_on_error=False,
        stats_only=False,
    )
    es.indices.refresh(index=index)
    elapsed = time.time() - start
    logger.info("✅ %d documents indexés dans '%s' en %.1fs (%d erreurs)", ok, index, elapsed, len(errors))
    return ok, errors


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="SciPulse — ingestion ArXiv (bulk Python)")
    parser.add_argument("source", nargs="?", help="Fichier JSON-lines local")
    parser.add_argument("--minio", action="store_true", help="Lire depuis MinIO (arxiv-raw/chunks/)")
    parser.add_argument("--index", default=IDX_ARXIV_RAW)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args(argv)

    if not args.minio and not args.source:
        parser.error("indiquer un fichier ou --minio")

    lines = iter_lines_minio() if args.minio else iter_lines_file(args.source)
    es = Elasticsearch(ES_HOST)
    _, errors = ingest(es, lines, args.index, args.limit)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
