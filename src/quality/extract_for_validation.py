"""
SciPulse — Extraction de données depuis Elasticsearch vers Pandas
pour validation par Great Expectations.

Usage :
    python -m src.quality.extract_for_validation
"""

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from src.utils.config import ES_HOST, IDX_ARXIV, IDX_HN

ARXIV_COLUMNS = [
    "arxiv_id", "title", "abstract", "authors_flat", "categories",
    "primary_category", "date_published", "date_updated", "doi",
]
ARXIV_ENRICHED_COLUMNS = ARXIV_COLUMNS + ["keywords", "hn_score", "hn_comments_count"]
HN_COLUMNS = [
    "hn_id", "type", "by", "time", "title", "url", "text", "score",
    "descendants", "arxiv_id_linked",
]


def extract_arxiv(
    es: Elasticsearch,
    index: str = IDX_ARXIV,
    max_docs: int = 10_000,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Extrait un échantillon d'articles ArXiv depuis Elasticsearch."""
    columns = columns or ARXIV_COLUMNS
    docs = []
    for hit in helpers.scan(es, index=index, query={"query": {"match_all": {}}}, size=1000):
        source = hit["_source"]
        docs.append({c: source.get(c) for c in columns})
        if len(docs) >= max_docs:
            break

    df = pd.DataFrame(docs, columns=columns)
    print(f"✅ Extrait {len(df):,} articles ArXiv depuis '{index}'")
    return df


def extract_hn(es: Elasticsearch, index: str = IDX_HN, max_docs: int = 5_000) -> pd.DataFrame:
    """Extrait les items Hacker News depuis Elasticsearch."""
    docs = []
    for hit in helpers.scan(es, index=index, query={"query": {"match_all": {}}}, size=500):
        s = hit["_source"]
        docs.append({
            "hn_id": s.get("id"),
            "type": s.get("type"),
            "by": s.get("by"),
            "time": s.get("time"),
            "title": s.get("title"),
            "url": s.get("url"),
            "text": s.get("text"),
            "score": s.get("score"),
            "descendants": s.get("descendants"),
            "arxiv_id_linked": s.get("arxiv_id"),
        })
        if len(docs) >= max_docs:
            break

    df = pd.DataFrame(docs, columns=HN_COLUMNS)
    print(f"✅ Extrait {len(df):,} items HN depuis '{index}'")
    return df


if __name__ == "__main__":
    es = Elasticsearch(ES_HOST)
    print("\n📥 Extraction des données pour validation\n")
    df_arxiv = extract_arxiv(es)
    print(df_arxiv.head(3).to_string())
    print("\n" + "─" * 60 + "\n")
    df_hn = extract_hn(es)
    print(df_hn.head(3).to_string())
