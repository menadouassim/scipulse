import json
from unittest.mock import patch

import pytest

from src.search.search_service import SearchService

EMPTY = {"hits": {"hits": [], "total": {"value": 0}}}


@pytest.fixture
def mock_es():
    with patch("src.search.search_service.Elasticsearch") as MockES:
        MockES.return_value.search.return_value = EMPTY
        yield MockES.return_value


def body_of(mock_es) -> dict:
    return mock_es.search.call_args[1]["body"]


def test_search_uses_multi_match_with_title_boost(mock_es):
    SearchService().search("reinforcement learning")
    body = json.dumps(body_of(mock_es))
    assert "multi_match" in body and "title^3" in body and "abstract" in body
    assert "highlight" in body


def test_search_applies_filters(mock_es):
    SearchService().search("x", categories=["cs.AI"], date_from="2020-01-01")
    body = json.dumps(body_of(mock_es))
    assert "cs.AI" in body and "2020-01-01" in body


def test_search_function_score_recency_and_hn(mock_es):
    SearchService().search("x")
    body = json.dumps(body_of(mock_es))
    assert "function_score" in body and "gauss" in body and "hn_score" in body


def test_search_without_boosts_is_plain_bool(mock_es):
    SearchService().search("x", recency_boost=False, hn_boost=False)
    assert "function_score" not in json.dumps(body_of(mock_es))


def test_phrase_slop(mock_es):
    SearchService().search_phrase("neural network architecture", slop=3)
    q = body_of(mock_es)["query"]["match_phrase"]["abstract"]
    assert q["slop"] == 3


def test_more_like_this_references_document(mock_es):
    SearchService().more_like_this("2301.07041", size=10)
    mlt = body_of(mock_es)["query"]["more_like_this"]
    assert mlt["like"][0]["_id"] == "2301.07041"
    assert body_of(mock_es)["size"] == 10


def test_significant_terms_parses_buckets(mock_es):
    mock_es.search.return_value = {"aggregations": {"sample": {"terms": {"buckets": [
        {"key": "malware", "score": 0.5, "doc_count": 10}]}}}}
    assert SearchService().significant_terms(category="cs.CR") == [
        {"term": "malware", "score": 0.5, "doc_count": 10}]
    assert "cs.CR" in json.dumps(body_of(mock_es))


def test_suggest_flattens_options(mock_es):
    mock_es.search.return_value = {"suggest": {"s": [{"options": [
        {"text": "convolutional neural network"}, {"text": "convolutional neural networks"}]}]}}
    assert SearchService().suggest("convlutional nueral network")[0] == "convolutional neural network"


def test_has_child_on_hn(mock_es):
    SearchService().hn_posts_with_comment("breakthrough")
    assert "has_child" in json.dumps(body_of(mock_es))
    assert mock_es.search.call_args[1]["index"] == "hn-items"


def test_format_hits(mock_es):
    mock_es.search.return_value = {"hits": {"total": {"value": 1}, "hits": [
        {"_id": "1", "_score": 2.0, "_source": {"title": "T"}, "highlight": {"abstract": ["<mark>x</mark>"]}}]}}
    r = SearchService().search("x")
    assert r["total"] == 1
    assert r["hits"][0]["title"] == "T" and r["hits"][0]["highlight"]["abstract"]
