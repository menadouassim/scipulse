"""
SciPulse — Générateur d'un échantillon synthétique au format du dump Kaggle ArXiv.

Permet de faire tourner toute la chaîne (et la CI) sans télécharger les 3,5 Go du dump.
Les textes sont synthétiques (vocabulaire par thématique) ; ~1 % de défauts volontaires
(abstract court, espaces multiples) donnent du grain à moudre à l'étape de nettoyage.

Usage :
    python -m src.ingestion.generate_sample --n 2000 --out data/arxiv-raw/arxiv-sample.json
"""

import argparse
import json
import random
from datetime import date, timedelta

TOPICS = {
    "cs.LG": (["neural network", "gradient descent", "representation learning", "transformer", "generalization",
               "regularization", "self-supervised learning", "optimization"], "learning"),
    "cs.AI": (["reinforcement learning", "planning", "knowledge graph", "reasoning", "large language model",
               "agents", "multi-agent", "RLHF"], "intelligence"),
    "cs.CL": (["language model", "machine translation", "named entity recognition", "attention mechanism",
               "pretraining", "question answering", "LLM", "tokenization"], "language"),
    "cs.CV": (["convolutional neural network", "image segmentation", "object detection", "diffusion",
               "vision transformer", "CNN", "video understanding", "depth estimation"], "vision"),
    "cs.CR": (["cryptography", "zero-knowledge proof", "adversarial attack", "privacy", "malware",
               "homomorphic encryption", "side channel", "authentication"], "security"),
    "cs.RO": (["robot manipulation", "reinforcement learning", "motion planning", "SLAM", "grasping",
               "legged locomotion", "sim-to-real", "robotics"], "robotics"),
    "stat.ML": (["Bayesian inference", "kernel methods", "variational inference", "causal inference",
                 "Gaussian process", "bandits", "generalization bound", "statistical learning"], "statistics"),
    "cs.DB": (["query optimization", "index structure", "stream processing", "data lake",
               "transaction", "graph database", "vector search", "full-text search"], "databases"),
}
FIRST = ["Alice", "Bob", "Chen", "Dmitri", "Elena", "Farid", "Grace", "Hiro", "Ines", "Jamal", "Kofi", "Lena",
         "Mei", "Noor", "Omar", "Priya", "Quentin", "Rosa", "Sven", "Tariq"]
LAST = ["Martin", "Nguyen", "Ivanov", "Garcia", "Kim", "Okafor", "Rossi", "Schmidt", "Tanaka", "Dubois",
        "Haddad", "Singh", "Silva", "Novak", "Larsen", "Chowdhury", "Moreau", "Kowalski", "Yamada", "Costa"]
TITLE_T = ["{a} for {b}", "Scaling {a} with {b}", "On the limits of {a}", "{a} meets {b}", "Towards robust {a}",
           "A survey of {a}", "Efficient {a} via {b}", "Rethinking {a} in {b}"]
ABS_T = [
    "We study {a} and its relation to {b}.",
    "In this paper, we propose a novel approach to {a} that leverages {b}.",
    "Our experiments show that {a} improves substantially over strong baselines.",
    "We demonstrate that {b} is crucial for {a} in realistic settings.",
    "Extensive evaluation on standard benchmarks confirms the effectiveness of {a}.",
    "We further analyze {b} and discuss implications for {domain}.",
]


def make_record(rng: random.Random, i: int, start: date, end: date) -> dict:
    cat = rng.choice(list(TOPICS))
    terms, domain = TOPICS[cat]
    a, b = rng.sample(terms, 2)
    extra = rng.choice([c for c in TOPICS if c != cat])
    published = start + timedelta(days=rng.randint(0, (end - start).days))
    updated = min(end, published + timedelta(days=rng.choice([0, 0, 5, 30, 200])))
    authors = [(rng.choice(LAST), rng.choice(FIRST), "") for _ in range(rng.randint(1, 5))]
    abstract = " ".join(rng.choice(ABS_T).format(a=a, b=b, domain=domain) for _ in range(rng.randint(3, 6)))
    title = rng.choice(TITLE_T).format(a=a.title(), b=b)
    arxiv_id = f"{published.year % 100:02d}{published.month:02d}.{i % 100000:05d}"

    r = rng.random()
    if r < 0.005:
        abstract = "Too short."
    elif r < 0.01:
        title = title.replace(" ", "  ", 1)

    return {
        "id": arxiv_id,
        "submitter": f"{authors[0][1]} {authors[0][0]}",
        "authors": ", ".join(f"{f} {l}" for l, f, _ in authors),
        "title": title,
        "comments": None,
        "journal-ref": None,
        "doi": f"10.1000/sample.{arxiv_id}" if rng.random() < 0.3 else None,
        "report-no": None,
        "categories": f"{cat} {extra}" if rng.random() < 0.5 else cat,
        "license": None,
        "abstract": f" {abstract}\n",
        "versions": [{"version": "v1", "created": published.strftime("%a, %-d %b %Y 12:00:00 GMT")}],
        "update_date": updated.isoformat(),
        "authors_parsed": [list(a) for a in authors],
    }


def generate(n: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    return [make_record(rng, i, date(2015, 1, 1), date(2025, 6, 30)) for i in range(n)]


def main() -> None:
    parser = argparse.ArgumentParser(description="SciPulse — échantillon ArXiv synthétique")
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="data/arxiv-raw/arxiv-sample.json")
    args = parser.parse_args()

    with open(args.out, "w", encoding="utf-8") as f:
        for rec in generate(args.n, args.seed):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"✅ {args.n} articles synthétiques écrits dans {args.out}")


if __name__ == "__main__":
    main()
