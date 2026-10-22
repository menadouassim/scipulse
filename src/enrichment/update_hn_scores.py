"""
SciPulse — Mise à jour des scores croisés ArXiv ↔ HN.

1. Rafraîchit score / commentaires des posts liés via l'API HN (best effort)
2. Répercute les agrégats dans `arxiv-papers` (hn_score, hn_comments_count, hn_item_ids)

Usage :
    python -m src.enrichment.update_hn_scores [--offline]
"""

import argparse
import logging
import sys

from elasticsearch import Elasticsearch, helpers

from src.enrichment.enrich_arxiv import load_hn_aggregates
from src.ingestion.hn_poller import fetch_item
from src.utils.config import ES_HOST, IDX_ARXIV, IDX_HN, IDX_LINKS

logger = logging.getLogger(__name__)


def refresh_links_from_api(es: Elasticsearch) -> int:
    """Met à jour score/commentaires des liens (et des items HN) depuis l'API."""
    actions = []
    for hit in helpers.scan(es, index=IDX_LINKS, query={"query": {"match_all": {}}}):
        item = fetch_item(hit["_source"]["hn_id"])
        if not item:
            continue
        fresh = {"hn_score": item.get("score", 0), "hn_comments_count": item.get("descendants", 0)}
        actions.append({"_op_type": "update", "_index": IDX_LINKS, "_id": hit["_id"], "doc": fresh})
        actions.append({
            "_op_type": "update", "_index": IDX_HN, "_id": str(hit["_source"]["hn_id"]),
            "doc": {"score": fresh["hn_score"], "descendants": fresh["hn_comments_count"]},
        })
    if actions:
        helpers.bulk(es, actions, raise_on_error=False)
        es.indices.refresh(index=IDX_LINKS)
    return len(actions) // 2


def push_to_papers(es: Elasticsearch) -> int:
    aggregates = load_hn_aggregates(es)
    actions = [
        {"_op_type": "update", "_index": IDX_ARXIV, "_id": arxiv_id, "doc": doc}
        for arxiv_id, doc in aggregates.items()
        if es.exists(index=IDX_ARXIV, id=arxiv_id)
    ]
    ok, _ = helpers.bulk(es, actions, raise_on_error=False)
    es.indices.refresh(index=IDX_ARXIV)
    return ok


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="SciPulse — mise à jour des scores ArXiv ↔ HN")
    parser.add_argument("--offline", action="store_true", help="Ne pas interroger l'API HN")
    args = parser.parse_args(argv)

    es = Elasticsearch(ES_HOST)
    if not args.offline:
        logger.info("🔄 %d liens rafraîchis depuis l'API HN", refresh_links_from_api(es))
    logger.info("📊 %d articles ArXiv mis à jour", push_to_papers(es))
    return 0


if __name__ == "__main__":
    sys.exit(main())
