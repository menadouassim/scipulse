"""Tests d'intégration — nécessitent Elasticsearch (docker compose up -d elasticsearch)."""

import json

import pytest
from elasticsearch import Elasticsearch

from src.ingestion.arxiv_bulk_ingest import ingest
from src.ingestion.generate_sample import generate
from src.ingestion.reindex_arxiv import reindex
from src.search.search_service import SearchService
from src.utils import mappings
from src.utils.config import ES_HOST

pytestmark = pytest.mark.integration

# Index dédiés aux tests : ne jamais toucher aux index de travail (arxiv-papers, hn-items…)
RAW, PAPERS = "it-arxiv-raw", "it-arxiv-papers"


@pytest.fixture(scope="module")
def es():
    client = Elasticsearch(ES_HOST)
    if not client.ping():
        pytest.skip("Elasticsearch injoignable")
    client.indices.delete(index=[RAW, PAPERS], ignore_unavailable=True)
    client.indices.create(
        index=PAPERS, settings=mappings.ARXIV_SETTINGS["settings"], mappings=mappings.ARXIV_SETTINGS["mappings"]
    )
    ingest(client, (json.dumps(r) for r in generate(300)), index=RAW)
    reindex(client, RAW, PAPERS)
    yield client
    client.indices.delete(index=[RAW, PAPERS], ignore_unavailable=True)


def test_counts_match(es):
    assert es.count(index=RAW)["count"] == es.count(index=PAPERS)["count"] == 300


def test_synonym_search_cnn_equals_expansion(es):
    svc = SearchService(index=PAPERS)
    assert svc.search("CNN", recency_boost=False, hn_boost=False)["total"] == \
        svc.search("convolutional neural network", recency_boost=False, hn_boost=False)["total"] > 0


def test_stemming_network_networks(es):
    svc = SearchService(index=PAPERS)
    assert svc.search("networks")["total"] == svc.search("network")["total"]


def test_more_like_this_and_suggest(es):
    svc = SearchService(index=PAPERS)
    some_id = es.search(index=PAPERS, size=1)["hits"]["hits"][0]["_id"]
    assert svc.more_like_this(some_id)["total"] > 0
    assert svc.suggest("convlutional nueral network")
