"""SciPulse — configuration partagée (variables d'environnement)."""

import os

from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "scipulse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "scipulse")
PG_DATABASE = os.getenv("PG_DATABASE", "scipulse")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")

IDX_ARXIV_RAW = "arxiv-papers-raw"
IDX_ARXIV = "arxiv-papers"
IDX_HN = "hn-items"
IDX_LINKS = "arxiv-hn-links"
IDX_LOGS = "pipeline-logs"
