import pandas as pd
import pytest

from src.quality import clean_arxiv, gx_runner, validate_arxiv, validate_hn
from src.quality.extract_for_validation import ARXIV_COLUMNS, HN_COLUMNS


def arxiv_df(**overrides) -> pd.DataFrame:
    row = {
        "arxiv_id": "2301.00001", "title": "A perfectly fine title", "abstract": "x" * 120,
        "authors_flat": ["A B"], "categories": ["cs.AI"], "primary_category": "cs.AI",
        "date_published": "2023-01-01", "date_updated": "2023-01-02", "doi": None,
    }
    rows = [{**row, "arxiv_id": f"2301.{i:05d}"} for i in range(20)]
    df = pd.DataFrame(rows, columns=ARXIV_COLUMNS)
    for k, v in overrides.items():
        df[k] = v
    return df


@pytest.fixture(autouse=True)
def tmp_gx_root(tmp_path, monkeypatch):
    monkeypatch.setattr(gx_runner, "GX_ROOT", tmp_path)


def test_clean_dedupes_normalises_and_flags():
    df = pd.DataFrame([
        {"arxiv_id": "1", "title": "Bad   title", "abstract": "line\nbreak " + "x" * 60,
         "authors_flat": ["A   B"], "categories": ["cs.AI"], "primary_category": None, "date_updated": "2020-01-01"},
        {"arxiv_id": "1", "title": "Bad   title", "abstract": "short", "authors_flat": ["A B"],
         "categories": ["cs.AI"], "primary_category": "cs.AI", "date_updated": "2021-01-01"},
        {"arxiv_id": "2", "title": "", "abstract": "y" * 80, "authors_flat": [], "categories": ["cs.LG"],
         "primary_category": "cs.LG", "date_updated": "2021-01-01"},
    ])
    out = clean_arxiv.clean(df)
    assert list(out["arxiv_id"]) == ["1"]  # doublon retiré (date la plus récente gardée), titre vide retiré
    assert out.loc[0, "title"] == "Bad title"
    assert bool(out.loc[0, "abstract_too_short"]) is True  # signalé, pas supprimé


def test_clean_rebuilds_primary_category_and_authors():
    df = pd.DataFrame([{"arxiv_id": "1", "title": "A title here", "abstract": "z" * 80,
                        "authors_flat": ["A    B"], "categories": ["cs.CL", "cs.AI"], "primary_category": None}])
    out = clean_arxiv.clean(df)
    assert out.loc[0, "primary_category"] == "cs.CL"
    assert out.loc[0, "authors_flat"] == ["A B"]


def test_diagnose_counts():
    df = arxiv_df()
    df.loc[0, "abstract"] = "tiny"
    df.loc[1, "title"] = "two  spaces"
    df.loc[2, "primary_category"] = "hep-ph"
    rep = clean_arxiv.diagnose(df)
    assert rep["short_abstracts"] == 1 and rep["multi_space_titles"] == 1 and rep["unknown_categories"] == 1


def test_arxiv_validation_passes_on_clean_data():
    result = validate_arxiv.validate_arxiv_data(arxiv_df())
    assert result.success


def test_arxiv_validation_detects_duplicates_and_nulls():
    df = arxiv_df()
    df.loc[1, "arxiv_id"] = df.loc[0, "arxiv_id"]
    df.loc[2, "title"] = None
    assert not validate_arxiv.validate_arxiv_data(df).success


def test_hn_validation():
    good = pd.DataFrame([{
        "hn_id": i, "type": "story", "by": "u", "time": "2024-05-01T10:00:00+00:00", "title": "t", "url": None,
        "text": None, "score": 3, "descendants": 1, "arxiv_id_linked": None} for i in range(5)], columns=HN_COLUMNS)
    assert validate_hn.validate_hn_data(good).success
    bad = good.copy()
    bad.loc[0, "type"] = "banana"
    bad.loc[1, "score"] = -5
    assert not validate_hn.validate_hn_data(bad).success
