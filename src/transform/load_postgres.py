"""
SciPulse — Miroir tabulaire PostgreSQL (source des modèles dbt).

Charge `arxiv-papers` et `arxiv-hn-links` d'Elasticsearch vers le schéma `public` :
  raw_arxiv_papers   (1 ligne / article)
  raw_arxiv_authors  (1 ligne / article x auteur)
  raw_arxiv_hn_links (1 ligne / lien ArXiv x post HN)

Usage :
    python -m src.transform.load_postgres
"""

import logging
import sys

import psycopg2
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import execute_values

from src.utils.config import (
    ES_HOST, IDX_ARXIV, IDX_LINKS,
    PG_DATABASE, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER,
)

logger = logging.getLogger(__name__)
PAGE = 5_000

DDL = """
-- Idempotent : les vues dbt dépendent de ces tables, on vide au lieu de les supprimer.
CREATE TABLE IF NOT EXISTS raw_arxiv_papers (
    arxiv_id          TEXT PRIMARY KEY,
    title             TEXT,
    abstract          TEXT,
    categories        TEXT,            -- séparées par des espaces (format ArXiv)
    primary_category  TEXT,
    date_published    DATE,
    date_updated      DATE,
    doi               TEXT
);
CREATE TABLE IF NOT EXISTS raw_arxiv_authors (
    arxiv_id    TEXT NOT NULL,
    position    INT  NOT NULL,
    author_name TEXT,
    PRIMARY KEY (arxiv_id, position)
);
CREATE TABLE IF NOT EXISTS raw_arxiv_hn_links (
    arxiv_id          TEXT NOT NULL,
    hn_id             BIGINT NOT NULL,
    hn_score          INT,
    hn_comments_count INT,
    PRIMARY KEY (arxiv_id, hn_id)
);
TRUNCATE raw_arxiv_papers, raw_arxiv_authors, raw_arxiv_hn_links;
"""


def get_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DATABASE
    )


def _flush(cur, sql: str, rows: list) -> None:
    if rows:
        execute_values(cur, sql, rows, page_size=PAGE)
        rows.clear()


def load_papers(es: Elasticsearch, cur) -> int:
    papers, authors, n = [], [], 0
    p_sql = (
        "INSERT INTO raw_arxiv_papers VALUES %s ON CONFLICT (arxiv_id) DO NOTHING"
    )
    a_sql = "INSERT INTO raw_arxiv_authors VALUES %s ON CONFLICT DO NOTHING"
    for hit in helpers.scan(es, index=IDX_ARXIV, query={"query": {"match_all": {}}}, size=1000):
        s = hit["_source"]
        papers.append((
            s["arxiv_id"], s.get("title"), s.get("abstract"),
            " ".join(s.get("categories") or []), s.get("primary_category"),
            s.get("date_published"), s.get("date_updated"), s.get("doi"),
        ))
        for pos, name in enumerate(s.get("authors_flat") or []):
            authors.append((s["arxiv_id"], pos, name))
        n += 1
        if len(papers) >= PAGE:
            _flush(cur, p_sql, papers)
            _flush(cur, a_sql, authors)
    _flush(cur, p_sql, papers)
    _flush(cur, a_sql, authors)
    return n


def load_links(es: Elasticsearch, cur) -> int:
    if not es.indices.exists(index=IDX_LINKS):
        return 0
    rows = [
        (s["arxiv_id"], s["hn_id"], s.get("hn_score"), s.get("hn_comments_count"))
        for s in (h["_source"] for h in helpers.scan(es, index=IDX_LINKS, query={"query": {"match_all": {}}}))
    ]
    execute_values(cur, "INSERT INTO raw_arxiv_hn_links VALUES %s ON CONFLICT DO NOTHING", rows)
    return len(rows)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    es = Elasticsearch(ES_HOST)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        n_papers = load_papers(es, cur)
        n_links = load_links(es, cur)
    logger.info("✅ PostgreSQL : %d articles, %d liens HN chargés", n_papers, n_links)
    return 0


if __name__ == "__main__":
    sys.exit(main())
