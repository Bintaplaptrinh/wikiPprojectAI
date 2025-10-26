"""Export cleaned population records from MongoDB into JSON Lines for Hadoop.

This script fetches the `wiki_pop_raw` collection from MongoDB and writes the
data into a JSON Lines file that can be copied into HDFS as the input for the
Hadoop ETL job. Fields are kept simple so the streaming mapper can clean and
bucket the values consistently.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
OUTPUT_PATH = Path(os.getenv("HADOOP_EXPORT_PATH", "/data/population_raw.jsonl"))
COLLECTION_NAME = os.getenv("HADOOP_COLLECTION", "wiki_pop_raw")


def stream_documents(client: MongoClient) -> Iterable[Dict[str, Any]]:
    """Yield documents from MongoDB without loading them all into memory."""
    collection = client[DB_NAME][COLLECTION_NAME]
    cursor = collection.find({}, {"_id": 0})
    for document in cursor:
        yield document


def export() -> None:
    if not MONGODB_URI or not DB_NAME:
        raise RuntimeError("Missing MONGODB_URI or DB_NAME in environment.")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    client = MongoClient(MONGODB_URI)
    try:
        with OUTPUT_PATH.open("w", encoding="utf-8") as handle:
            for document in stream_documents(client):
                handle.write(json.dumps(document, ensure_ascii=False))
                handle.write("\n")
        print(f"Wrote Hadoop export to {OUTPUT_PATH.resolve()}")
    finally:
        client.close()


if __name__ == "__main__":
    export()
