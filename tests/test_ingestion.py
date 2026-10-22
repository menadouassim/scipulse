import json

from src.ingestion.arxiv_bulk_ingest import generate_actions, transform_raw
from src.ingestion.generate_sample import generate
from src.ingestion.prepare_dump import keep
from src.ingestion.reindex_arxiv import build_authors, parse_version_date, transform
from src.ingestion.upload_to_minio import iter_chunks


def test_transform_raw_mirrors_logstash_filters():
    doc = transform_raw({
        "id": "0704.0001", "update_date": "2008-11-26", "categories": "hep-ph cs.AI",
        "title": "  A  title\n", "abstract": " multi\nline  abstract ",
    })
    assert doc["arxiv_id"] == "0704.0001" and "id" not in doc
    assert doc["date_updated"] == "2008-11-26"
    assert doc["categories"] == ["hep-ph", "cs.AI"] and doc["primary_category"] == "hep-ph"
    assert doc["title"] == "A title" and doc["abstract"] == "multi line abstract"


def test_transform_raw_requires_id():
    assert transform_raw({"title": "x"}) is None


def test_generate_actions_skips_bad_lines_and_honours_limit():
    lines = [json.dumps({"id": "1"}), "not json", json.dumps({"id": "2"}), json.dumps({"id": "3"})]
    actions = list(generate_actions(lines, "idx", limit=3))
    assert [a["_id"] for a in actions] == ["1", "2"]
    assert all(a["_index"] == "idx" for a in actions)


def test_parse_version_date():
    assert parse_version_date("Mon, 2 Apr 2007 19:18:42 GMT") == "2007-04-02"
    assert parse_version_date("garbage") is None
    assert parse_version_date(None) is None


def test_build_authors_from_parsed_and_string():
    nested, flat = build_authors({"authors_parsed": [["Ballesteros", "Ana", ""], ["Li", "Wei", "", "MIT"]]})
    assert flat == ["Ana Ballesteros", "Wei Li"]
    assert nested[1]["affiliation"] == "MIT"
    _, flat2 = build_authors({"authors": "A. One, B. Two and C. Three"})
    assert flat2 == ["A. One", "B. Two", "C. Three"]


def test_reindex_transform_full_record():
    out = transform({
        "arxiv_id": "1", "title": "T", "abstract": "A", "categories": ["cs.AI", "cs.LG"],
        "versions": [{"created": "Tue, 3 Jan 2023 10:00:00 GMT"}], "date_updated": "2023-05-01",
        "authors_parsed": [["Doe", "Jane", ""]],
    })
    assert out["date_published"] == "2023-01-03" and out["primary_category"] == "cs.AI"
    assert out["authors_flat"] == ["Jane Doe"]


def test_reindex_transform_falls_back_to_update_date():
    assert transform({"arxiv_id": "1", "date_updated": "2020-02-02T00:00:00.000Z"})["date_published"] == "2020-02-02"


def test_iter_chunks(tmp_path):
    f = tmp_path / "d.json"
    f.write_text("\n".join(f'{{"id": {i}}}' for i in range(5)) + "\n\n")
    assert [len(c) for c in iter_chunks(str(f), 2)] == [2, 2, 1]


def test_prepare_dump_keep():
    assert keep("cs.AI math.OC") and keep("stat.ML") and not keep("hep-ph math.AG")


def test_sample_generator_is_deterministic_and_dump_shaped():
    a, b = generate(50, seed=1), generate(50, seed=1)
    assert a == b
    rec = a[0]
    assert {"id", "title", "abstract", "categories", "update_date", "versions", "authors_parsed"} <= rec.keys()
    assert len({r["id"] for r in generate(500)}) == 500
