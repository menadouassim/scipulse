"""
SciPulse — Détection des liens ArXiv dans les items HN déjà indexés.

Second passage après le poller : repère les liens ArXiv dans `url` ET dans le
texte des items (ex. commentaires), et crée les documents manquants dans `arxiv-hn-links`.

Usage :
    python -m src.enrichment.detect_arxiv_links
"""

import logging
import sys
from datetime import datetime, timezone

from elasticsearch import Elasticsearch, helpers

from src.ingestion.hn_poller import ARXIV_URL_PATTERN, extract_arxiv_id
from src.utils.config import ES_HOST, IDX_HN, IDX_LINKS

logger = logging.getLogger(__name__)


def find_arxiv_ids(item: dict) -> set[str]:
    """Tous les ids ArXiv mentionnés dans l'url et le texte d'un item HN."""
    found = set()
    direct = extract_arxiv_id(item.get("url"))
    if direct:
        found.add(direct)
    for text in (item.get("text"), item.get("title")):
        if text:
            for m in ARXIV_URL_PATTERN.finditer(text):
                found.add(m.group(1).rsplit("v", 1)[0] if "v" in m.group(1) else m.group(1))
    return found


def detect(es: Elasticsearch) -> int:
    query = {"query": {"bool": {"should": [
        {"exists": {"field": "arxiv_id"}},
        {"match_phrase": {"text": "arxiv.org"}},
        {"wildcard": {"url": "*arxiv.org*"}},
    ], "minimum_should_match": 1}}}
    now = datetime.now(timezone.utc).isoformat()

    def actions():
        for hit in helpers.scan(es, index=IDX_HN, query=query):
            item = hit["_source"]
            for arxiv_id in find_arxiv_ids(item):
                doc = {
                    "arxiv_id": arxiv_id,
                    "hn_id": item["id"],
                    "hn_title": item.get("title"),
                    "hn_score": item.get("score", 0),
                    "hn_comments_count": item.get("descendants", 0),
                    "url": item.get("url"),
                    "detected_at": now,
                }
                if item.get("time"):
                    doc["hn_time"] = item["time"]
                yield {"_index": IDX_LINKS, "_id": f"{arxiv_id}_{item['id']}", "_source": doc}

    ok, errors = helpers.bulk(es, actions(), raise_on_error=False)
    es.indices.refresh(index=IDX_LINKS)
    logger.info("🔗 %d lien(s) ArXiv ↔ HN (ré)indexés (%d erreurs)", ok, len(errors))
    return ok


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    detect(Elasticsearch(ES_HOST))
    return 0


if __name__ == "__main__":
    sys.exit(main())
