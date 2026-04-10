import os
import time

import pandas as pd
import ray
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, BooleanType
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError


# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
ICEBERG_WAREHOUSE = "gs://ray-platform-v2-ray2-data-bucket/iceberg"
ICEBERG_NAMESPACE = "corp"
ICEBERG_TABLE = "corporate_registry"
ICEBERG_TABLE_IDENTIFIER = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE}"
ICEBERG_TABLE_LOCATION = f"{ICEBERG_WAREHOUSE}/{ICEBERG_NAMESPACE}/{ICEBERG_TABLE}"
CATALOG_URI = "sqlite:////tmp/iceberg_catalog.db"


# ──────────────────────────────────────────────
# Ray Task: parallel ingestion
# ──────────────────────────────────────────────
@ray.remote(num_cpus=1)
def ingest_data(source_name: str) -> pd.DataFrame:
    """Ingest data from a CSV stored in GCS."""
    env_var = f"{source_name}_PATH"
    path = os.getenv(env_var)

    if not path:
        raise RuntimeError(f"Environment variable {env_var} is not set")

    print(f"--- [Task] Ingesting data from {source_name} at {path} ---")
    df = pd.read_csv(path)
    print(f"Ingested {len(df)} records from {source_name}")
    return df


# ──────────────────────────────────────────────
# Ray Actor: EntityResolver pinned to worker node
# ──────────────────────────────────────────────
@ray.remote(num_gpus=0, resources={"entity_resolver": 1})
class EntityResolver:
    """Simulated LLM-based Entity Resolution actor."""

    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY", "NOT_SET")
        if self.api_key == "NOT_SET":
            print("WARNING: LLM_API_KEY not found. Resolution will use simulated mode.")
        print("--- [Actor] EntityResolver initialised on worker node ---")

    def resolve_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        print(f"--- [Actor] Processing batch of {len(df)} records via simulated LLM ---")
        time.sleep(3)

        df = df.copy()

        if "corporate_name" not in df.columns:
            if "company_name" in df.columns:
                df["corporate_name"] = df["company_name"]
            elif "name" in df.columns:
                df["corporate_name"] = df["name"]
            else:
                raise RuntimeError(
                    "Required column 'corporate_name' not found in input data."
                )

        df["corporate_name"] = df["corporate_name"].astype(str)
        df["canonical_id"] = df["corporate_name"].apply(
            lambda x: f"UID-{abs(hash(x)) % 10000:04d}"
        )
        df["is_resolved"] = True

        print(f"--- [Actor] Resolution complete. {len(df)} records resolved ---")
        return df


# ──────────────────────────────────────────────
# Iceberg sink
# ──────────────────────────────────────────────
def write_to_iceberg(resolved_df: pd.DataFrame) -> None:
    """Write resolved DataFrame to Apache Iceberg table on GCS using PyIceberg."""
    print(f"--- [Sink] Writing to Apache Iceberg table {ICEBERG_TABLE_IDENTIFIER} ---")

    required_cols = ["corporate_name", "canonical_id", "is_resolved"]
    missing_cols = [c for c in required_cols if c not in resolved_df.columns]
    if missing_cols:
        raise RuntimeError(f"Missing required columns for Iceberg sink: {missing_cols}")

    sink_df = resolved_df[required_cols].copy()
    sink_df["corporate_name"] = sink_df["corporate_name"].astype(str)
    sink_df["canonical_id"] = sink_df["canonical_id"].astype(str)
    sink_df["is_resolved"] = sink_df["is_resolved"].astype(bool)

    arrow_table = pa.Table.from_pandas(sink_df, preserve_index=False)

    catalog = SqlCatalog(
        "corp_catalog",
        **{
            "uri": CATALOG_URI,
            "warehouse": ICEBERG_WAREHOUSE,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )

    try:
        catalog.create_namespace(ICEBERG_NAMESPACE)
        print(f"Created Iceberg namespace: {ICEBERG_NAMESPACE}")
    except NamespaceAlreadyExistsError:
        print(f"Namespace '{ICEBERG_NAMESPACE}' already exists — continuing.")

    schema = Schema(
        NestedField(1, "corporate_name", StringType(), required=False),
        NestedField(2, "canonical_id", StringType(), required=False),
        NestedField(3, "is_resolved", BooleanType(), required=False),
    )

    try:
        table = catalog.create_table(
            identifier=ICEBERG_TABLE_IDENTIFIER,
            schema=schema,
            location=ICEBERG_TABLE_LOCATION,
        )
        print(f"Created Iceberg table: {ICEBERG_TABLE_IDENTIFIER}")
    except TableAlreadyExistsError:
        table = catalog.load_table(ICEBERG_TABLE_IDENTIFIER)
        print(f"Loaded existing Iceberg table: {ICEBERG_TABLE_IDENTIFIER}")

    table.append(arrow_table)

    print(f"Successfully committed {len(sink_df)} records to Iceberg table.")
    print(f"Table location: {ICEBERG_TABLE_LOCATION}")


# ──────────────────────────────────────────────
# Pipeline entrypoint
# ──────────────────────────────────────────────
def run_pipeline() -> None:
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    print("=" * 50)
    print("Pipeline Execution Started.")
    print("=" * 50)

    future_a = ingest_data.remote("SUPPLYCHAIN_DATA_S1")
    future_b = ingest_data.remote("FINANCIAL_DATA_S2")

    data_sources = ray.get([future_a, future_b])
    combined_df = pd.concat(data_sources, ignore_index=True)
    print(f"Ingested total of {len(combined_df)} records.")

    print("Starting LLM-based Entity Resolution...")
    resolver = EntityResolver.remote()
    resolved_df = ray.get(resolver.resolve_batch.remote(combined_df))

    print("--- Harmonized Data Summary ---")
    print(
        resolved_df[["corporate_name", "canonical_id", "is_resolved"]]
        .head(10)
        .to_string(index=False)
    )

    # Automatic Iceberg write on every pipeline run
    write_to_iceberg(resolved_df)

    print("=" * 50)
    print("Pipeline Execution Complete.")
    print("=" * 50)


if __name__ == "__main__":
    run_pipeline()