"""
SciPulse — Mappings Elasticsearch.

Usage :
    python -m src.utils.mappings            # crée les index manquants
    python -m src.utils.mappings --recreate # supprime puis recrée
"""

import argparse

from elasticsearch import Elasticsearch

from src.utils.config import ES_HOST, IDX_ARXIV, IDX_HN, IDX_LINKS, IDX_LOGS

DATE_FORMAT = "strict_date_optional_time||yyyy-MM-dd||epoch_second"

_ANALYSIS = {
    "filter": {
        "english_stemmer": {"type": "stemmer", "language": "english"},
        "english_stop": {"type": "stop", "stopwords": "_english_"},
        "scientific_synonyms": {
            "type": "synonym",
            "synonyms": [
                "cnn, convolutional neural network",
                "rnn, recurrent neural network",
                "llm, large language model",
                "rl, reinforcement learning",
                "gan, generative adversarial network",
                "nlp, natural language processing",
                "gnn, graph neural network",
                "rlhf, reinforcement learning from human feedback",
                "ml, machine learning",
                "dl, deep learning",
            ],
        },
    },
    "analyzer": {
        "scientific_english": {
            "type": "custom",
            "tokenizer": "standard",
            # synonymes AVANT le stemmer
            "filter": ["lowercase", "english_stop", "scientific_synonyms", "english_stemmer"],
        }
    },
}

ARXIV_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": _ANALYSIS,
    },
    "mappings": {
        "properties": {
            "arxiv_id": {"type": "keyword"},
            "title": {
                "type": "text",
                "analyzer": "scientific_english",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 512},
                    "suggest": {"type": "completion", "analyzer": "simple"},
                    "trigram": {"type": "text", "analyzer": "standard"},
                },
            },
            "abstract": {
                "type": "text",
                "analyzer": "scientific_english",
                "term_vector": "with_positions_offsets",
                "fields": {"trigram": {"type": "text", "analyzer": "standard"}},
            },
            "authors": {
                "type": "nested",
                "properties": {
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "affiliation": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                },
            },
            "authors_flat": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "categories": {"type": "keyword"},
            "primary_category": {"type": "keyword"},
            "date_published": {"type": "date", "format": DATE_FORMAT},
            "date_updated": {"type": "date", "format": DATE_FORMAT},
            "doi": {"type": "keyword"},
            "keywords": {"type": "keyword"},
            "hn_score": {"type": "integer"},
            "hn_comments_count": {"type": "integer"},
            "hn_item_ids": {"type": "keyword"},
        }
    },
}

HN_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0, "analysis": _ANALYSIS},
    "mappings": {
        "properties": {
            "id": {"type": "long"},
            "type": {"type": "keyword"},
            "by": {"type": "keyword"},
            "time": {"type": "date", "format": DATE_FORMAT},
            "title": {
                "type": "text",
                "analyzer": "scientific_english",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
            },
            "text": {"type": "text", "analyzer": "scientific_english"},
            "url": {"type": "keyword"},
            "score": {"type": "integer"},
            "descendants": {"type": "integer"},
            "parent": {"type": "long"},
            "post_kind": {"type": "keyword"},  # story | show_hn | ask_hn | comment | job | poll
            "arxiv_id": {"type": "keyword"},
            "indexed_at": {"type": "date"},
            "relation": {
                "type": "join",
                "relations": {"story": "comment"},
            },
        }
    },
}

LINKS_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "arxiv_id": {"type": "keyword"},
            "hn_id": {"type": "long"},
            "hn_title": {"type": "text"},
            "hn_score": {"type": "integer"},
            "hn_comments_count": {"type": "integer"},
            "hn_time": {"type": "date", "format": DATE_FORMAT},
            "url": {"type": "keyword"},
            "detected_at": {"type": "date"},
        }
    },
}

LOGS_SETTINGS = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "dag_id": {"type": "keyword"},
            "task_id": {"type": "keyword"},
            "run_id": {"type": "keyword"},
            "status": {"type": "keyword"},
            "duration_s": {"type": "float"},
            "docs_processed": {"type": "long"},
            "message": {"type": "text"},
        }
    },
}

INDICES = {
    IDX_ARXIV: ARXIV_SETTINGS,
    IDX_HN: HN_SETTINGS,
    IDX_LINKS: LINKS_SETTINGS,
    IDX_LOGS: LOGS_SETTINGS,
}


def create_indices(
    es: Elasticsearch, recreate: bool = False, names: list[str] | None = None
) -> dict[str, str]:
    """
    Crée les index SciPulse (tous, ou ceux de `names`).
    Retourne {index: 'created'|'recreated'|'exists'}.
    """
    result = {}
    for name, body in INDICES.items():
        if names is not None and name not in names:
            continue
        exists = es.indices.exists(index=name)
        if exists and recreate:
            es.indices.delete(index=name)
            state = "recreated"
        elif exists:
            result[name] = "exists"
            continue
        else:
            state = "created"
        es.indices.create(index=name, settings=body["settings"], mappings=body["mappings"])
        result[name] = state
    return result


def main():
    parser = argparse.ArgumentParser(description="SciPulse — déploiement des mappings")
    parser.add_argument("--recreate", action="store_true", help="Supprime puis recrée les index")
    args = parser.parse_args()

    es = Elasticsearch(ES_HOST)
    for name, state in create_indices(es, recreate=args.recreate).items():
        print(f"  ✅ Index '{name}' {state}.")
    print("\nMappings déployés avec succès.")


if __name__ == "__main__":
    main()
