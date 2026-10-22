"""
SciPulse — Upload des données ArXiv vers MinIO.
Découpe le dump JSON-lines en chunks et les dépose dans le bucket arxiv-raw.

Usage :
    python -m src.ingestion.upload_to_minio data/arxiv-raw/arxiv-cs-subset-100k.json
"""

import os
import sys
from io import BytesIO

import boto3
from botocore.client import Config

from src.utils.config import MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY

BUCKET = "arxiv-raw"


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(s3, bucket: str = BUCKET) -> None:
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)


def upload_file(s3, local_path: str, object_name: str | None = None) -> str:
    object_name = object_name or os.path.basename(local_path)
    s3.upload_file(local_path, BUCKET, object_name)
    print(f"  ✅ {os.path.basename(local_path)} → s3://{BUCKET}/{object_name}")
    return object_name


def iter_chunks(source_file: str, chunk_size: int):
    """Génère des listes de `chunk_size` lignes JSON non vides."""
    buffer: list[str] = []
    with open(source_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            buffer.append(line)
            if len(buffer) >= chunk_size:
                yield buffer
                buffer = []
    if buffer:
        yield buffer


def split_and_upload(s3, source_file: str, chunk_size: int = 10_000, prefix: str = "chunks/") -> int:
    """Découpe un fichier JSON-lines en chunks et les upload. Retourne le nb d'articles."""
    print(f"\n📦 Découpage en chunks de {chunk_size:,} articles...")
    total = 0
    n_chunks = 0
    for n_chunks, lines in enumerate(iter_chunks(source_file, chunk_size), start=1):
        object_name = f"{prefix}arxiv-chunk-{n_chunks - 1:04d}.json"
        s3.upload_fileobj(BytesIO("\n".join(lines).encode("utf-8")), BUCKET, object_name)
        print(f"  ✅ {object_name} ({len(lines):,} articles)")
        total += len(lines)
    print(f"\n🏁 {n_chunks} chunks uploadés ({total:,} articles au total)")
    return total


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    source = argv[0] if argv else "data/arxiv-raw/arxiv-cs-subset-100k.json"

    if not os.path.exists(source):
        print(f"❌ Fichier introuvable : {source}")
        return 1

    print(f"📤 Upload de {source} vers MinIO (bucket: {BUCKET})")
    print(f"   Endpoint: {MINIO_ENDPOINT}\n")

    s3 = get_s3()
    ensure_bucket(s3)
    upload_file(s3, source)
    split_and_upload(s3, source)
    return 0


if __name__ == "__main__":
    sys.exit(main())
