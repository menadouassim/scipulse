from unittest.mock import MagicMock

from src.enrichment.detect_arxiv_links import find_arxiv_ids
from src.enrichment.enrich_arxiv import extract_keywords, load_hn_aggregates


def test_keywords_are_document_specific():
    texts = [
        "convolutional networks for image segmentation and image classification",
        "zero knowledge proofs for private authentication and encryption",
        "bayesian inference with gaussian processes",
    ]
    kws = extract_keywords(texts, top_k=3)
    assert len(kws) == 3 and all(len(k) <= 3 for k in kws)
    assert any("image" in k for k in kws[0])
    assert any(w in " ".join(kws[1]) for w in ("proofs", "authentication", "knowledge", "encryption"))


def test_keywords_empty_input():
    assert extract_keywords([]) == []


def test_find_arxiv_ids_in_url_and_text():
    item = {"url": "https://arxiv.org/abs/2301.07041v2",
            "text": 'see <a href="https://arxiv.org/pdf/1706.03762">paper</a> and arxiv.org/abs/2005.14165'}
    assert find_arxiv_ids(item) == {"2301.07041", "1706.03762", "2005.14165"}
    assert find_arxiv_ids({"url": "https://example.com"}) == set()


def test_load_hn_aggregates(monkeypatch):
    es = MagicMock()
    es.indices.exists.return_value = True
    hits = [{"_source": {"arxiv_id": "A", "hn_id": 1, "hn_score": 10, "hn_comments_count": 2}},
            {"_source": {"arxiv_id": "A", "hn_id": 2, "hn_score": 50, "hn_comments_count": 5}},
            {"_source": {"arxiv_id": "B", "hn_id": 3, "hn_score": 1, "hn_comments_count": 0}}]
    monkeypatch.setattr("src.enrichment.enrich_arxiv.helpers.scan", lambda *a, **k: iter(hits))
    agg = load_hn_aggregates(es)
    assert agg["A"] == {"hn_score": 50, "hn_comments_count": 7, "hn_item_ids": ["1", "2"]}
    assert agg["B"]["hn_score"] == 1
