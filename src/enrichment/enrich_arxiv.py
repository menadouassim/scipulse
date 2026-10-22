"""
SciPulse — Enrichissement des articles ArXiv.

1. Mots-clés : TF-IDF (scikit-learn) sur les abstracts -> champ `keywords`
2. Croisement HN : `hn_score`, `hn_comments_count`, `hn_item_ids` depuis `arxiv-hn-links`

Usage :
    python -m src.enrichment.enrich_arxiv
    python -m src.enrichment.enrich_arxiv --top-k 8
"""

import argparse
import logging
import sys
from collections import defaultdict

from elasticsearch import Elasticsearch, helpers
from sklearn.feature_extraction.text import TfidfVectorizer

from src.utils.config import ES_HOST, IDX_ARXIV, IDX_LINKS

logger = logging.getLogger(__name__)

# Mots génériques d'abstracts scientifiques, ajoutés aux stopwords anglais
EXTRA_STOPWORDS = {
    "paper", "propose", "proposed", "present", "show", "shows", "results", "result", "approach",
    "method", "methods", "based", "using", "use", "used", "new", "novel", "study", "work",
    "also", "can", "however", "demonstrate", "demonstrated", "experiments", "experimental",
    "performance", "problem", "problems", "model", "models", "data", "different", "existing",
}


def extract_keywords(texts: list[str], top_k: int = 5) -> list[list[str]]:
    """TF-IDF léger : retourne les `top_k` termes les plus saillants de chaque texte."""
    if not texts:
        return []
    from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

    vectorizer = TfidfVectorizer(
        stop_words=list(ENGLISH_STOP_WORDS | EXTRA_STOPWORDS),
        ngram_range=(1, 2),
        min_df=1,
        max_df=0.5 if len(texts) > 5 else 1.0,
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z\-]{2,}\b",
        max_features=50_000,
        sublinear_tf=True,
    )
    matrix = vectorizer.fit_transform(texts)
    vocab = vectorizer.get_feature_names_out()
    out = []
    for i in range(matrix.shape[0]):
        row = matrix.getrow(i)
        if row.nnz == 0:
            out.append([])
            continue
        order = row.data.argsort()[::-1][:top_k]
        out.append([vocab[row.indices[j]] for j in order])
    return out


def load_hn_aggregates(es: Elasticsearch) -> dict[str, dict]:
    """Agrège arxiv-hn-links par arxiv_id (score max, commentaires cumulés, ids HN)."""
    agg: dict[str, dict] = defaultdict(lambda: {"hn_score": 0, "hn_comments_count": 0, "hn_item_ids": []})
    if not es.indices.exists(index=IDX_LINKS):
        return {}
    for hit in helpers.scan(es, index=IDX_LINKS, query={"query": {"match_all": {}}}):
        s = hit["_source"]
        a = agg[s["arxiv_id"]]
        a["hn_score"] = max(a["hn_score"], s.get("hn_score") or 0)
        a["hn_comments_count"] += s.get("hn_comments_count") or 0
        a["hn_item_ids"].append(str(s["hn_id"]))
    return dict(agg)


def enrich(es: Elasticsearch, top_k: int = 5, index: str = IDX_ARXIV) -> int:
    ids, texts = [], []
    for hit in helpers.scan(es, index=index, query={"query": {"match_all": {}}, "_source": ["title", "abstract"]}):
        s = hit["_source"]
        ids.append(hit["_id"])
        texts.append(f"{s.get('title') or ''}. {s.get('abstract') or ''}")
    logger.info("🧠 %d articles à enrichir", len(ids))
    if not ids:
        return 0

    keywords = extract_keywords(texts, top_k=top_k)
    hn = load_hn_aggregates(es)
    logger.info("🔗 %d articles liés à Hacker News", len(hn))

    def actions():
        for arxiv_id, kws in zip(ids, keywords):
            doc = {"keywords": kws, "hn_score": 0, "hn_comments_count": 0}
            doc.update(hn.get(arxiv_id, {}))
            yield {"_op_type": "update", "_index": index, "_id": arxiv_id, "doc": doc}

    ok, errors = helpers.bulk(es, actions(), chunk_size=1000, raise_on_error=False)
    es.indices.refresh(index=index)
    logger.info("✅ %d articles enrichis (%d erreurs)", ok, len(errors))
    return ok


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="SciPulse — enrichissement ArXiv")
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args(argv)
    enrich(Elasticsearch(ES_HOST), top_k=args.top_k)
    return 0


if __name__ == "__main__":
    sys.exit(main())
