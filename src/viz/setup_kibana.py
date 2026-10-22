"""
SciPulse — Création des data views et du dashboard Kibana « SciPulse ».

Utilise l'API Saved Objects (idempotent : `overwrite=true`, ids fixes).

Usage :
    python -m src.viz.setup_kibana
    python -m src.viz.setup_kibana --export docker/kibana/scipulse-dashboard.ndjson
"""

import argparse
import json
import sys

import requests

from src.utils.config import IDX_ARXIV, IDX_HN, IDX_LINKS, IDX_LOGS, KIBANA_HOST

LENS_IDS = {"scipulse-v-total", "scipulse-v-hnlinks", "scipulse-v-hnscore"}

HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

DATA_VIEWS = {
    "scipulse-arxiv": (IDX_ARXIV, "date_published"),
    "scipulse-hn": (IDX_HN, "time"),
    "scipulse-links": (IDX_LINKS, "hn_time"),
    "scipulse-logs": (IDX_LOGS, "@timestamp"),
}


# ── Briques de construction des visualisations « aggs-based » ────────────────

def _agg(id_, type_, schema, params=None):
    return {"id": id_, "enabled": True, "type": type_, "schema": schema, "params": params or {}}


def _count(id_="1"):
    return _agg(id_, "count", "metric")


def _terms(id_, field, size=5, schema="group", order_by="1"):
    return _agg(id_, "terms", schema, {
        "field": field, "size": size, "order": "desc", "orderBy": order_by,
        "otherBucket": False, "missingBucket": False,
    })


def _date_hist(id_, field, interval="M"):
    return _agg(id_, "date_histogram", "segment", {
        "field": field, "interval": interval, "min_doc_count": 0, "useNormalizedEsInterval": True,
    })


def _vis(id_, title, vis_type, aggs, data_view, params=None, description=""):
    default_params = {
        "area": {"type": "area", "grid": {"categoryLines": False}, "legendPosition": "right",
                 "addTooltip": True, "addLegend": True, "mode": "stacked", "interpolate": "linear",
                 "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                                   "show": True, "scale": {"type": "linear"}, "labels": {"show": True},
                                   "title": {}}],
                 "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left",
                                "show": True, "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True}, "title": {"text": "Publications"}}],
                 "seriesParams": [{"show": True, "type": "area", "mode": "stacked", "data": {"label": "Count", "id": "1"},
                                   "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
                                   "showCircles": False, "interpolate": "linear"}]},
        "line": {"type": "line", "grid": {"categoryLines": False}, "legendPosition": "right",
                 "addTooltip": True, "addLegend": True,
                 "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                                   "show": True, "scale": {"type": "linear"}, "labels": {"show": True}, "title": {}}],
                 "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "left",
                                "show": True, "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True}, "title": {"text": "Score HN moyen"}}],
                 "seriesParams": [{"show": True, "type": "line", "mode": "normal", "data": {"label": "Score HN moyen", "id": "1"},
                                   "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
                                   "showCircles": True, "interpolate": "linear"}]},
        "horizontal_bar": {"type": "histogram", "grid": {"categoryLines": False}, "legendPosition": "right",
                           "addTooltip": True, "addLegend": False,
                           "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "left",
                                             "show": True, "scale": {"type": "linear"}, "labels": {"show": True}, "title": {}}],
                           "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value", "position": "bottom",
                                          "show": True, "scale": {"type": "linear", "mode": "normal"},
                                          "labels": {"show": True}, "title": {"text": "Documents"}}],
                           "seriesParams": [{"show": True, "type": "histogram", "mode": "normal",
                                             "data": {"label": "Count", "id": "1"}, "valueAxis": "ValueAxis-1"}]},
        "metric": {"addTooltip": True, "addLegend": False, "type": "metric",
                   "metric": {"percentageMode": False, "useRanges": False, "colorSchema": "Green to Red",
                              "metricColorMode": "None", "labels": {"show": True},
                              "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "", "fontSize": 40}}},
        "pie": {"type": "pie", "addTooltip": True, "addLegend": True, "legendPosition": "right",
                "isDonut": True, "labels": {"show": True, "values": True, "last_level": True, "truncate": 100}},
        "tagcloud": {"scale": "linear", "orientation": "single", "minFontSize": 18, "maxFontSize": 72, "showLabel": True},
        "table": {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                  "showTotal": False, "totalFunc": "sum", "percentageCol": ""},
    }
    base = default_params.get(vis_type, {})
    vis_state = {"title": title, "type": vis_type, "aggs": aggs, "params": {**base, **(params or {})}}
    return {
        "type": "visualization", "id": id_,
        "attributes": {
            "title": title, "description": description, "visState": json.dumps(vis_state),
            "uiStateJSON": "{}", "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                "query": {"query": "", "language": "kuery"}, "filter": [],
                "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
            })},
        },
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern", "id": data_view}],
    }


def _lens_metric(id_: str, title: str, data_view: str, operation: str, field: str | None = None) -> dict:
    """Tuile « metric » Lens (le type aggs-based `metric` est déprécié dans Kibana 8)."""
    column = {
        "label": title, "dataType": "number", "operationType": operation, "isBucketed": False,
        "scale": "ratio", "sourceField": field or "___records___", "params": {"emptyAsNull": False},
    }
    state = {
        "datasourceStates": {"formBased": {"layers": {"l1": {
            "columns": {"c1": column}, "columnOrder": ["c1"], "incompleteColumns": {},
        }}}},
        "visualization": {"layerId": "l1", "layerType": "data", "metricAccessor": "c1"},
        "query": {"query": "", "language": "kuery"}, "filters": [],
    }
    return {
        "type": "lens", "id": id_,
        "attributes": {"title": title, "description": "", "visualizationType": "lnsMetric", "state": state},
        "references": [{"type": "index-pattern", "id": data_view, "name": "indexpattern-datasource-layer-l1"}],
    }


def build_objects() -> list[dict]:
    objs = []
    for dv_id, (pattern, time_field) in DATA_VIEWS.items():
        objs.append({
            "type": "index-pattern", "id": dv_id,
            "attributes": {"title": pattern, "name": pattern, "timeFieldName": time_field},
        })

    vis = [
        _vis("scipulse-v-volume", "Volume mensuel par catégorie (top 5)", "area",
             [_count(), _date_hist("2", "date_published"), _terms("3", "primary_category", 5)], "scipulse-arxiv"),
        _vis("scipulse-v-sigterms", "Top 20 mots-clés (TF-IDF des abstracts)", "horizontal_bar",
             [_count(), _terms("2", "keywords", 20, schema="segment")], "scipulse-arxiv",
             description="Mots-clés TF-IDF les plus fréquents (champ keywords)"),
        _lens_metric("scipulse-v-total", "Articles ArXiv indexés", "scipulse-arxiv", "count"),
        _lens_metric("scipulse-v-hnlinks", "Posts HN liés à ArXiv", "scipulse-links", "count"),
        _lens_metric("scipulse-v-hnscore", "Score HN moyen (articles liés)", "scipulse-links", "average", "hn_score"),
        _vis("scipulse-v-table", "Articles les plus discutés sur HN", "table",
             [_agg("1", "max", "metric", {"field": "hn_comments_count"}),
              _terms("2", "arxiv_id", 10, schema="bucket", order_by="1")], "scipulse-links"),
        _vis("scipulse-v-tagcloud", "Mots-clés émergents", "tagcloud",
             [_count(), _terms("2", "keywords", 50, schema="segment")], "scipulse-arxiv"),
        _vis("scipulse-v-hnweekly", "Score HN moyen par semaine", "line",
             [_agg("1", "avg", "metric", {"field": "hn_score"}), _date_hist("2", "hn_time", "w")], "scipulse-links"),
        _vis("scipulse-v-hntypes", "Types de posts HN", "pie",
             [_count(), _terms("2", "post_kind", 10, schema="segment")], "scipulse-hn"),
        _vis("scipulse-v-taskstatus", "Pipeline — succès / échecs des tâches", "pie",
             [_count(), _terms("2", "status", 5, schema="segment")], "scipulse-logs"),
        _vis("scipulse-v-taskduration", "Pipeline — durée moyenne par tâche (s)", "horizontal_bar",
             [_agg("1", "avg", "metric", {"field": "duration_s"}), _terms("2", "task_id", 10, schema="segment", order_by="1")],
             "scipulse-logs"),
        _vis("scipulse-v-taskdocs", "Pipeline — documents traités par tâche", "horizontal_bar",
             [_agg("1", "sum", "metric", {"field": "docs_processed"}), _terms("2", "task_id", 10, schema="segment", order_by="1")],
             "scipulse-logs"),
    ]
    objs += vis

    layout = [
        ("scipulse-v-total", 0, 0, 12, 8), ("scipulse-v-hnlinks", 12, 0, 12, 8), ("scipulse-v-hnscore", 24, 0, 12, 8),
        ("scipulse-v-hntypes", 36, 0, 12, 8),
        ("scipulse-v-volume", 0, 8, 48, 16),
        ("scipulse-v-sigterms", 0, 24, 24, 16), ("scipulse-v-tagcloud", 24, 24, 24, 16),
        ("scipulse-v-table", 0, 40, 24, 16), ("scipulse-v-hnweekly", 24, 40, 24, 16),
    ]
    monitoring = [
        ("scipulse-v-taskstatus", 0, 0, 16, 14), ("scipulse-v-taskduration", 16, 0, 16, 14),
        ("scipulse-v-taskdocs", 32, 0, 16, 14),
    ]
    objs.append(_dashboard("scipulse-dashboard", "SciPulse", layout))
    objs.append(_dashboard("scipulse-monitoring", "SciPulse — Monitoring du pipeline", monitoring))
    return objs


def _dashboard(id_: str, title: str, layout: list[tuple]) -> dict:
    panels, refs = [], []
    for i, (vid, x, y, w, h) in enumerate(layout):
        kind = "lens" if vid in LENS_IDS else "visualization"
        panels.append({
            "version": "8.13.4", "type": kind,
            "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i)},
            "panelIndex": str(i), "embeddableConfig": {}, "panelRefName": f"panel_{i}",
        })
        refs.append({"name": f"panel_{i}", "type": kind, "id": vid})
    return {
        "type": "dashboard", "id": id_,
        "attributes": {
            "title": title, "description": "Veille scientifique ArXiv × Hacker News",
            "panelsJSON": json.dumps(panels), "optionsJSON": json.dumps({"useMargins": True, "hidePanelTitles": False}),
            "timeRestore": True, "timeFrom": "now-30y", "timeTo": "now", "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})},
        },
        "references": refs,
    }


def deploy(kibana: str = KIBANA_HOST) -> dict:
    resp = requests.post(
        f"{kibana}/api/saved_objects/_bulk_create?overwrite=true",
        headers=HEADERS, data=json.dumps(build_objects()), timeout=60,
    )
    resp.raise_for_status()
    errors = [o for o in resp.json()["saved_objects"] if "error" in o]
    if errors:
        raise RuntimeError(f"Erreurs Kibana : {errors}")
    return resp.json()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SciPulse — déploiement du dashboard Kibana")
    parser.add_argument("--export", help="Écrit un fichier ndjson importable (Stack Management > Saved Objects)")
    args = parser.parse_args(argv)

    if args.export:
        with open(args.export, "w", encoding="utf-8") as f:
            for obj in build_objects():
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        print(f"📄 Export écrit dans {args.export}")
        return 0

    deploy()
    print(f"✅ Dashboards déployés : {KIBANA_HOST}/app/dashboards#/view/scipulse-dashboard")
    return 0


if __name__ == "__main__":
    sys.exit(main())
