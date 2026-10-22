"""
SciPulse — Service de recherche full-text.

Encapsule les requêtes avancées sur `arxiv-papers` et `hn-items` :
recherche pondérée (multi_match + function_score), phrase avec slop, more_like_this,
significant_terms, suggestion (did-you-mean), has_child sur les commentaires HN.

Usage :
    from src.search.search_service import SearchService
    svc = SearchService()
    svc.search("reinforcement learning robotics", categories=["cs.RO"])
"""

from elasticsearch import Elasticsearch

from src.utils.config import ES_HOST, IDX_ARXIV, IDX_HN, IDX_LINKS

HIGHLIGHT = {
    "pre_tags": ["<mark>"],
    "post_tags": ["</mark>"],
    "fields": {"abstract": {"fragment_size": 200, "number_of_fragments": 2}},
}


class SearchService:
    def __init__(self, host: str = ES_HOST, index: str = IDX_ARXIV):
        self.es = Elasticsearch(host)
        self.index = index

    # ── Recherche principale ───────────────────────────────────────────────

    def search(
        self,
        query: str,
        categories: list[str] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        size: int = 10,
        recency_boost: bool = True,
        hn_boost: bool = True,
    ) -> dict:
        """
        multi_match titre^3 + abstract, filtres catégorie / dates, highlight,
        pondération par fraîcheur (decay gaussien) et popularité HN.
        """
        bool_query: dict = {
            "must": [
                {"multi_match": {
                    "query": query,
                    "fields": ["title^3", "abstract"],
                    "type": "cross_fields",
                    "operator": "and",
                }}
            ],
            "filter": [],
        }
        if categories:
            bool_query["filter"].append({"terms": {"categories": categories}})
        if date_from or date_to:
            rng = {k: v for k, v in (("gte", date_from), ("lte", date_to)) if v}
            bool_query["filter"].append({"range": {"date_published": rng}})

        functions = []
        if recency_boost:
            functions.append(
                {"gauss": {"date_published": {"origin": "now", "scale": "730d", "decay": 0.5}}}
            )
        if hn_boost:
            functions.append(
                {"field_value_factor": {"field": "hn_score", "modifier": "log1p", "missing": 0, "factor": 0.1}}
            )

        query_body: dict = {"bool": bool_query}
        if functions:
            query_body = {"function_score": {
                "query": query_body, "functions": functions,
                "score_mode": "sum", "boost_mode": "multiply",
            }}

        body = {"query": query_body, "highlight": HIGHLIGHT, "size": size}
        return self._format(self.es.search(index=self.index, body=body))

    def search_phrase(self, phrase: str, slop: int = 2, size: int = 10) -> dict:
        """match_phrase tolérant : les mots peuvent être séparés de `slop` positions."""
        body = {
            "query": {"match_phrase": {"abstract": {"query": phrase, "slop": slop}}},
            "highlight": HIGHLIGHT,
            "size": size,
        }
        return self._format(self.es.search(index=self.index, body=body))

    # ── Similarité ─────────────────────────────────────────────────────────

    def more_like_this(self, arxiv_id: str, size: int = 10) -> dict:
        """Les `size` articles les plus proches d'un article donné."""
        body = {
            "query": {"more_like_this": {
                "fields": ["title", "abstract"],
                "like": [{"_index": self.index, "_id": arxiv_id}],
                "min_term_freq": 1,
                "min_doc_freq": 1,
                "max_query_terms": 25,
            }},
            "size": size,
            "_source": ["arxiv_id", "title", "primary_category", "date_published"],
        }
        return self._format(self.es.search(index=self.index, body=body))

    # ── Agrégations ────────────────────────────────────────────────────────

    def significant_terms(
        self,
        category: str | None = None,
        date_from: str | None = None,
        size: int = 20,
        field: str = "abstract",
    ) -> list[dict]:
        """
        Termes qui distinguent un sous-ensemble (catégorie / période) du corpus.

        `significant_text` est l'équivalent de `significant_terms` pour les champs `text`
        (pas de fielddata nécessaire) ; il tokenise le texte à la volée.
        """
        filters = []
        if category:
            filters.append({"term": {"categories": category}})
        if date_from:
            filters.append({"range": {"date_published": {"gte": date_from}}})
        body = {
            "size": 0,
            "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
            "aggs": {"sample": {
                "sampler": {"shard_size": 5000},
                "aggs": {"terms": {"significant_text": {
                    "field": field, "size": size, "filter_duplicate_text": True,
                }}},
            }},
        }
        resp = self.es.search(index=self.index, body=body)
        buckets = resp["aggregations"]["sample"]["terms"]["buckets"]
        return [{"term": b["key"], "score": b["score"], "doc_count": b["doc_count"]} for b in buckets]

    def top_categories(self, size: int = 20) -> list[dict]:
        body = {"size": 0, "aggs": {"cats": {"terms": {"field": "primary_category", "size": size}}}}
        resp = self.es.search(index=self.index, body=body)
        return [{"category": b["key"], "count": b["doc_count"]} for b in resp["aggregations"]["cats"]["buckets"]]

    # ── Suggestion ─────────────────────────────────────────────────────────

    def suggest(self, text: str, size: int = 3) -> list[str]:
        """Did-you-mean (phrase suggester) : « convlutional nueral » → « convolutional neural »."""
        body = {"suggest": {"s": {
            "text": text,
            "phrase": {
                "field": "title.trigram",
                "size": size,
                "direct_generator": [{"field": "title.trigram", "suggest_mode": "popular"}],
            },
        }}}
        resp = self.es.search(index=self.index, body=body, size=0)
        return [opt["text"] for entry in resp["suggest"]["s"] for opt in entry["options"]]

    def autocomplete(self, prefix: str, size: int = 5) -> list[str]:
        body = {"suggest": {"c": {"prefix": prefix, "completion": {"field": "title.suggest", "size": size}}}}
        resp = self.es.search(index=self.index, body=body, size=0)
        return [opt["text"] for entry in resp["suggest"]["c"] for opt in entry["options"]]

    # ── Hacker News ────────────────────────────────────────────────────────

    def hn_posts_with_comment(self, term: str, size: int = 10) -> dict:
        """Stories HN dont au moins un commentaire contient `term` (has_child)."""
        body = {
            "query": {"has_child": {
                "type": "comment",
                "query": {"match": {"text": term}},
                "inner_hits": {"size": 3, "_source": ["text", "by"]},
            }},
            "size": size,
        }
        return self._format(self.es.search(index=IDX_HN, body=body))

    def most_discussed_papers(self, size: int = 10) -> list[dict]:
        """Articles ArXiv les plus discutés sur HN (index de liens)."""
        body = {"size": size, "sort": [{"hn_comments_count": "desc"}, {"hn_score": "desc"}]}
        resp = self.es.search(index=IDX_LINKS, body=body)
        return [h["_source"] for h in resp["hits"]["hits"]]

    # ── Utilitaires ────────────────────────────────────────────────────────

    @staticmethod
    def _format(resp: dict) -> dict:
        hits = resp["hits"]["hits"]
        return {
            "total": resp["hits"]["total"]["value"],
            "hits": [
                {
                    "id": h["_id"],
                    "score": h.get("_score"),
                    **h.get("_source", {}),
                    **({"highlight": h["highlight"]} if "highlight" in h else {}),
                    **({"inner_hits": h["inner_hits"]} if "inner_hits" in h else {}),
                }
                for h in hits
            ],
        }
