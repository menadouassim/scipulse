"""
SciPulse — Contrôle de santé du pipeline + alerte Watcher.

Usage :
    python -m src.viz.monitoring              # rapport de santé (code retour ≠ 0 si dégradé)
    python -m src.viz.monitoring --watcher    # installe une alerte Watcher (licence trial requise)
"""

import argparse
import sys
from datetime import datetime, timezone

from elasticsearch import Elasticsearch

from src.utils.config import ES_HOST, IDX_ARXIV, IDX_HN, IDX_LINKS, IDX_LOGS

HN_MAX_AGE_MIN = 30
ERROR_RATE_THRESHOLD = 0.2


def health_report(es: Elasticsearch) -> dict:
    report: dict = {"indices": {}, "alerts": []}
    for idx in (IDX_ARXIV, IDX_HN, IDX_LINKS, IDX_LOGS):
        report["indices"][idx] = es.count(index=idx)["count"] if es.indices.exists(index=idx) else None
        if report["indices"][idx] in (None, 0) and idx in (IDX_ARXIV,):
            report["alerts"].append(f"Index '{idx}' vide ou absent")

    # Fraîcheur HN
    if es.indices.exists(index=IDX_HN):
        latest = es.search(index=IDX_HN, size=1, sort=[{"indexed_at": "desc"}], _source=["indexed_at"])
        hits = latest["hits"]["hits"]
        if hits:
            ts = datetime.fromisoformat(hits[0]["_source"]["indexed_at"].replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - ts).total_seconds() / 60
            report["hn_age_min"] = round(age, 1)
            if age > HN_MAX_AGE_MIN:
                report["alerts"].append(f"Dernier item HN indexé il y a {age:.0f} min (> {HN_MAX_AGE_MIN})")

    # Taux d'erreur des tâches
    if es.indices.exists(index=IDX_LOGS):
        agg = es.search(index=IDX_LOGS, size=0, aggs={"s": {"terms": {"field": "status"}}})
        counts = {b["key"]: b["doc_count"] for b in agg["aggregations"]["s"]["buckets"]}
        total = sum(counts.values())
        if total:
            rate = counts.get("failed", 0) / total
            report["error_rate"] = round(rate, 3)
            if rate > ERROR_RATE_THRESHOLD:
                report["alerts"].append(f"Taux d'échec des tâches : {rate:.0%}")
    return report


def install_watcher(es: Elasticsearch) -> None:
    es.watcher.put_watch(
        id="scipulse-task-failures",
        trigger={"schedule": {"interval": "5m"}},
        input={"search": {"request": {"indices": [IDX_LOGS], "body": {
            "size": 0,
            "query": {"bool": {"filter": [
                {"term": {"status": "failed"}},
                {"range": {"@timestamp": {"gte": "now-15m"}}},
            ]}},
        }}}},
        condition={"compare": {"ctx.payload.hits.total": {"gte": 3}}},
        actions={"log_failures": {"logging": {
            "text": "SciPulse : {{ctx.payload.hits.total}} échecs de tâches sur les 15 dernières minutes"
        }}},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SciPulse — monitoring")
    parser.add_argument("--watcher", action="store_true")
    args = parser.parse_args(argv)
    es = Elasticsearch(ES_HOST)

    if args.watcher:
        es.license.post_start_trial(acknowledge=True)
        install_watcher(es)
        print("✅ Watch 'scipulse-task-failures' installé")
        return 0

    report = health_report(es)
    print("📋 État du pipeline")
    for idx, n in report["indices"].items():
        print(f"   {idx:<18} {n if n is not None else 'absent'}")
    for key in ("hn_age_min", "error_rate"):
        if key in report:
            print(f"   {key:<18} {report[key]}")
    for alert in report["alerts"]:
        print(f"   🚨 {alert}")
    return 1 if report["alerts"] else 0


if __name__ == "__main__":
    sys.exit(main())
