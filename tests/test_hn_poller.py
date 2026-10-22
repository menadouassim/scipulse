from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import hn_poller as hn


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://arxiv.org/abs/2301.07041", "2301.07041"),
        ("https://arxiv.org/pdf/1706.03762v5", "1706.03762"),
        ("http://www.arxiv.org/abs/2005.14165v1", "2005.14165"),
        ("https://example.com/arxiv", None),
        ("https://arxiv.org/list/cs.AI/recent", None),
        (None, None),
    ],
)
def test_extract_arxiv_id(url, expected):
    assert hn.extract_arxiv_id(url) == expected


def test_prepare_story_doc():
    doc = hn.prepare_hn_doc(
        {"id": 1, "type": "story", "by": "a", "time": 1700000000, "title": "Show HN: thing",
         "url": "https://arxiv.org/abs/2301.07041", "score": 12, "descendants": 3}
    )
    assert doc["relation"] == "story"
    assert doc["post_kind"] == "show_hn"
    assert doc["arxiv_id"] == "2301.07041"
    assert doc["time"].startswith("2023-11-14")


def test_prepare_comment_doc_has_join_relation():
    doc = hn.prepare_hn_doc({"id": 2, "type": "comment", "parent": 1, "text": "nice", "time": 1700000000})
    assert doc["relation"] == {"name": "comment", "parent": "1"}
    assert doc["post_kind"] == "comment"
    assert "arxiv_id" not in doc


def test_index_batch_routes_comments_and_creates_links():
    items = [
        {"id": 1, "type": "story", "title": "Paper", "url": "https://arxiv.org/abs/2301.07041", "score": 5, "descendants": 1, "time": 1700000000},
        {"id": 2, "type": "comment", "parent": 1, "text": "c", "time": 1700000001},
        {"id": 3, "deleted": True},
        None,
    ]
    with patch.object(hn.helpers, "bulk", return_value=(2, [])) as bulk:
        assert hn.index_batch(MagicMock(), items) == 2

    first_call, second_call = bulk.call_args_list
    actions = first_call.args[1]
    assert [a["_id"] for a in actions] == ["1", "2"]
    assert actions[1]["routing"] == "1"
    assert "routing" not in actions[0]
    links = second_call.args[1]
    assert links[0]["_index"] == hn.ES_INDEX_LINKS
    assert links[0]["_id"] == "2301.07041_1"


def test_poll_once_skips_known_items():
    es = MagicMock()
    with patch.object(hn, "fetch_new_story_ids", return_value=[1, 2, 3]), \
         patch.object(hn, "item_exists", side_effect=lambda _es, i: i == 1), \
         patch.object(hn, "fetch_items", return_value=[{"id": 2}, {"id": 3}]) as fetch, \
         patch.object(hn, "fetch_top_comments", return_value=[]), \
         patch.object(hn, "index_batch", return_value=2) as index:
        assert hn.poll_once(es) == 2
    fetch.assert_called_once_with([2, 3])
    index.assert_called_once()


def test_poll_once_nothing_new():
    with patch.object(hn, "fetch_new_story_ids", return_value=[1]), \
         patch.object(hn, "item_exists", return_value=True):
        assert hn.poll_once(MagicMock()) == 0


def test_fetch_item_returns_none_on_error():
    with patch.object(hn.requests, "get", side_effect=RuntimeError("boom")):
        assert hn.fetch_item(1) is None
