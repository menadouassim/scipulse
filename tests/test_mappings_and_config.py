from unittest.mock import MagicMock

from src.utils import mappings


def test_synonyms_precede_stemmer():
    filters = mappings.ARXIV_SETTINGS["settings"]["analysis"]["analyzer"]["scientific_english"]["filter"]
    assert filters.index("scientific_synonyms") < filters.index("english_stemmer")


def test_arxiv_mapping_key_fields():
    props = mappings.ARXIV_SETTINGS["mappings"]["properties"]
    assert props["categories"]["type"] == "keyword"
    assert props["authors"]["type"] == "nested"
    assert props["abstract"]["term_vector"] == "with_positions_offsets"
    assert "suggest" in props["title"]["fields"]


def test_hn_mapping_has_join():
    rel = mappings.HN_SETTINGS["mappings"]["properties"]["relation"]
    assert rel["type"] == "join" and rel["relations"] == {"story": "comment"}


def test_create_indices_is_idempotent():
    es = MagicMock()
    es.indices.exists.return_value = True
    assert set(mappings.create_indices(es).values()) == {"exists"}
    es.indices.create.assert_not_called()
    assert set(mappings.create_indices(es, recreate=True).values()) == {"recreated"}


def test_create_indices_can_be_scoped():
    es = MagicMock()
    es.indices.exists.return_value = True
    assert mappings.create_indices(es, recreate=True, names=["arxiv-papers"]) == {"arxiv-papers": "recreated"}
    es.indices.delete.assert_called_once_with(index="arxiv-papers")
