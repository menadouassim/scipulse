"""
SciPulse — Hacker News Poller
Polling de l'API Firebase HN et indexation dans Elasticsearch.

Usage :
    python -m src.ingestion.hn_poller              # un seul cycle
    python -m src.ingestion.hn_poller --continuous  # boucle toutes les 60s
"""

import argparse
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
from elasticsearch import Elasticsearch, helpers

from src.utils.config import ES_HOST, IDX_HN, IDX_LINKS

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

HN_API_BASE = "https://hacker-news.firebaseio.com/v0"
ES_INDEX_HN = IDX_HN
ES_INDEX_LINKS = IDX_LINKS
ARXIV_URL_PATTERN = re.compile(r"arxiv\.org/(?:abs|pdf)/(\d{4}\.\d{4,5}(?:v\d+)?)")
BATCH_SIZE = 50
POLL_INTERVAL = 60  # secondes
COMMENTS_PER_STORY = 5  # commentaires de premier niveau récupérés par story
HTTP_TIMEOUT = 10
FETCH_WORKERS = 10  # l'API Firebase tolère ~30 req/s


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST)


def fetch_new_story_ids() -> list[int]:
    """
    Récupère les 500 derniers IDs de stories depuis l'API HN.
    """
    resp = requests.get(f"{HN_API_BASE}/newstories.json", timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json() or []


def fetch_item(item_id: int) -> dict | None:
    """Récupère le détail d'un item HN."""
    try:
        resp = requests.get(f"{HN_API_BASE}/item/{item_id}.json", timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Erreur fetch item {item_id}: {e}")
        return None


def fetch_items(item_ids: list[int]) -> list[dict | None]:
    """Récupère plusieurs items en parallèle (ordre conservé)."""
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        return list(pool.map(fetch_item, item_ids))


def fetch_top_comments(stories: list[dict | None], per_story: int = COMMENTS_PER_STORY) -> list[dict]:
    """Récupère les premiers commentaires (niveau 1) des stories, pour la relation parent-child."""
    kid_ids = [
        kid for story in stories if story for kid in (story.get("kids") or [])[:per_story]
    ]
    return [c for c in fetch_items(kid_ids) if c and c.get("type") == "comment"]


def extract_arxiv_id(url: str | None) -> str | None:
    """
    Extrait l'ID ArXiv depuis une URL si elle pointe vers arxiv.org.
    """
    if not url:
        return None
    match = ARXIV_URL_PATTERN.search(url)
    if not match:
        return None
    return re.sub(r"v\d+$", "", match.group(1))


def item_exists(es: Elasticsearch, item_id: int) -> bool:
    """
    Vérifie si un item HN est déjà indexé.
    """
    # Les stories sont indexées sans routing : un HEAD direct suffit.
    return bool(es.exists(index=ES_INDEX_HN, id=str(item_id)))


def _post_kind(item: dict) -> str:
    if item.get("type") == "story":
        title = (item.get("title") or "").lower()
        if title.startswith("show hn"):
            return "show_hn"
        if title.startswith("ask hn"):
            return "ask_hn"
        return "story"
    return item.get("type", "unknown")


def prepare_hn_doc(item: dict) -> dict:
    """
    Transforme un item HN brut en document ES.
    """
    doc = {
        "id": item["id"],
        "type": item.get("type"),
        "by": item.get("by"),
        "title": item.get("title"),
        "text": item.get("text"),
        "url": item.get("url"),
        "score": item.get("score", 0),
        "descendants": item.get("descendants", 0),
        "post_kind": _post_kind(item),
        "indexed_at": datetime.now(timezone.utc).isoformat(),
    }
    if item.get("time") is not None:
        doc["time"] = datetime.fromtimestamp(item["time"], tz=timezone.utc).isoformat()

    # Relation parent-child pour les commentaires
    if item.get("type") == "comment" and item.get("parent") is not None:
        doc["parent"] = item["parent"]
        doc["relation"] = {"name": "comment", "parent": str(item["parent"])}
    else:
        doc["relation"] = "story"

    # Détection de lien ArXiv
    arxiv_id = extract_arxiv_id(item.get("url"))
    if arxiv_id:
        doc["arxiv_id"] = arxiv_id

    return {k: v for k, v in doc.items() if v is not None}


def index_batch(es: Elasticsearch, items: list[dict]) -> int:
    """Indexe un batch de documents HN dans Elasticsearch."""
    actions = []
    links = []

    for item in items:
        if not item or not item.get("id") or item.get("deleted") or item.get("dead"):
            continue

        # 1. Préparer le document
        doc = prepare_hn_doc(item)

        # 2. Action d'indexation HN
        action = {"_index": ES_INDEX_HN, "_id": str(item["id"]), "_source": doc}

        # 3. Routing pour les commentaires (parent-child)
        if isinstance(doc["relation"], dict):
            action["routing"] = doc["relation"]["parent"]
        actions.append(action)

        # 4. Si lien ArXiv détecté → créer aussi un document de liaison
        if doc.get("arxiv_id"):
            link_doc = {
                "arxiv_id": doc["arxiv_id"],
                "hn_id": item["id"],
                "hn_title": item.get("title"),
                "hn_score": item.get("score", 0),
                "hn_comments_count": item.get("descendants", 0),
                "url": item.get("url"),
                "detected_at": doc["indexed_at"],
            }
            if "time" in doc:
                link_doc["hn_time"] = doc["time"]
            links.append(
                {"_index": ES_INDEX_LINKS, "_id": f"{doc['arxiv_id']}_{item['id']}", "_source": link_doc}
            )

    if not actions:
        return 0

    # 5. Procéder à l'indexation des documents
    success, errors = helpers.bulk(es, actions, raise_on_error=False)
    if links:
        helpers.bulk(es, links, raise_on_error=False)

    if errors:
        logger.warning(f"{len(errors)} erreurs lors de l'indexation")

    if links:
        logger.info(f"  🔗 {len(links)} lien(s) ArXiv détecté(s)")

    return success


def poll_once(es: Elasticsearch) -> int:
    """
    Exécute un cycle de polling : récupère les nouvelles stories et les indexe.
    """

    # 1. Récupération des billets depuis l'API Hacker News
    story_ids = fetch_new_story_ids()
    logger.info(f"📥 {len(story_ids)} IDs de stories récupérés")

    # 2. Filtrer les items déjà indexés
    new_ids = [i for i in story_ids if not item_exists(es, i)]
    logger.info(f"🆕 {len(new_ids)} nouvelles stories à indexer")

    if not new_ids:
        return 0

    total_indexed = 0
    for i in range(0, len(new_ids), BATCH_SIZE):
        # 3. Indexation des nouveaux billets
        items = fetch_items(new_ids[i : i + BATCH_SIZE])
        total_indexed += index_batch(es, items + fetch_top_comments(items))

    return total_indexed


def main():
    parser = argparse.ArgumentParser(description="SciPulse — HN Poller")
    parser.add_argument(
        "--continuous", action="store_true", help="Boucle de polling continue"
    )
    parser.add_argument(
        "--interval", type=int, default=POLL_INTERVAL, help="Intervalle en secondes"
    )
    args = parser.parse_args()

    es = get_es_client()
    logger.info(f"Connexion ES: {es.info()['cluster_name']}")

    if args.continuous:
        logger.info(
            f"🔄 Mode continu — polling toutes les {args.interval}s (Ctrl+C pour arrêter)"
        )
        while True:
            try:
                poll_once(es)
                time.sleep(args.interval)
            except KeyboardInterrupt:
                logger.info("⏹  Arrêt du poller.")
                break
    else:
        total = poll_once(es)
        logger.info(f"🏁 Cycle terminé — {total} documents indexés au total.")


if __name__ == "__main__":
    main()
