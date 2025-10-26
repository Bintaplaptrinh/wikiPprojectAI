"""Train a simple clustering model on the country dataset using PySpark.

Usage::

    spark-submit spark/train_country_clusters.py \
        --input data/pop_clean.jsonl \
        --output data/spark/training_summary.json \
        --clusters 4

The script reads the cleaned dataset produced by the Hadoop ETL step and fits a
K-Means model. A compact summary containing the cluster size, centroid, and
squared error is stored in JSON so the web API can display the machine learning
result in the report.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train KMeans clusters for countries")
    parser.add_argument("--input", required=True, help="Path to the cleaned JSONL dataset")
    parser.add_argument("--output", required=True, help="Path to write the training summary JSON")
    parser.add_argument("--clusters", type=int, default=4, help="Number of clusters to compute")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CountryClusterTrainer")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_dataset(spark: SparkSession, path: str):
    df = spark.read.json(path)
    required = ["Country", "population", "density", "area_km2"]
    for column in required:
        if column not in df.columns:
            raise ValueError(f"Missing column in dataset: {column}")
    cleaned = (
        df.select("Country", "population", "density", "area_km2")
        .dropna()
        .where((col("population") > 0) & (col("density") > 0) & (col("area_km2") > 0))
    )
    return cleaned


def train_model(dataset, k: int):
    assembler = VectorAssembler(
        inputCols=["population", "density", "area_km2"],
        outputCol="features_raw",
    )
    assembled = assembler.transform(dataset)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    scaled = scaler.fit(assembled).transform(assembled)

    model = KMeans(k=k, featuresCol="features", seed=42)
    fitted = model.fit(scaled)
    transformed = fitted.transform(scaled)

    return fitted, transformed


def build_summary(model, predictions) -> Dict[str, Any]:
    cluster_counts = (
        predictions.groupBy("prediction")
        .count()
        .orderBy("prediction")
        .collect()
    )

    counts = {row["prediction"]: row["count"] for row in cluster_counts}
    centers = [list(center) for center in model.clusterCenters()]

    summary: Dict[str, Any] = {
        "algorithm": "KMeans",
        "k": len(centers),
        "within_set_sum_of_squared_errors": model.summary.trainingCost,
        "clusters": [],
    }

    for index, center in enumerate(centers):
        summary["clusters"].append(
            {
                "cluster_id": index,
                "center_population": center[0],
                "center_density": center[1],
                "center_area_km2": center[2],
                "country_count": counts.get(index, 0),
            }
        )

    return summary


def write_summary(path: str, payload: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    spark = build_spark()
    try:
        dataset = load_dataset(spark, args.input)
        model, predictions = train_model(dataset, args.clusters)
        summary = build_summary(model, predictions)
        write_summary(args.output, summary)
        print(f"Training finished. Summary written to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
