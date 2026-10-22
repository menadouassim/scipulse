"""
SciPulse — Extrait un sous-ensemble du dump Kaggle ArXiv (cs.* et stat.ML).

Usage :
    python -m src.ingestion.prepare_dump data/arxiv-metadata-oai-snapshot.json data/arxiv-raw/arxiv-cs-subset.json
    python -m src.ingestion.prepare_dump SRC DST --limit 100000
"""

import argparse
import json


def keep(categories: str) -> bool:
    cats = categories.split()
    return any(c.startswith("cs.") or c == "stat.ML" for c in cats)


def main() -> None:
    parser = argparse.ArgumentParser(description="SciPulse — sous-ensemble du dump ArXiv")
    parser.add_argument("src")
    parser.add_argument("dst")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    kept = 0
    with open(args.src, encoding="utf-8") as fin, open(args.dst, "w", encoding="utf-8") as fout:
        for line in fin:
            if args.limit is not None and kept >= args.limit:
                break
            if not line.strip():
                continue
            rec = json.loads(line)
            if keep(rec.get("categories", "")):
                fout.write(line if line.endswith("\n") else line + "\n")
                kept += 1
    print(f"✅ {kept:,} articles conservés dans {args.dst}")


if __name__ == "__main__":
    main()
