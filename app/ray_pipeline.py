import os
import time

import pandas as pd
import ray
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, BooleanType, LongType
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError


# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
ICEBERG_WAREHOUSE = "gs://ray-platform-v2-ray2-data-bucket/iceberg"
CATALOG_URI       = "sqlite:////tmp/iceberg_catalog.db"


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
    """Simulated LLM-based Entity Resolution actor.
    
    Pinned to worker node pool via custom Ray resource 'entity_resolver'.
    Worker nodes advertise this resource in rayStartParams.
    """

    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY", "NOT_SET")
        if self.api_key == "NOT_SET":
            print(
                "WARNING: LLM_API_KEY not found. "
                "Resolution will use simulated mode."
            )
        print("--- [Actor] EntityResolver initialised on worker node ---")

    def resolve_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        print(f"--- [Actor] Processing batch of {len(df)} records via simulated LLM ---")
        time.sleep(3)
        df = df.copy()
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
    print("--- [Sink] Writing to Apache Iceberg table corp.corporate_registry ---")

    # 1. Keep only the columns we care about
    cols = [c for c in ["corporate_name", "canonical_id", "is_resolved"] 
            if c in resolved_df.columns]
    sink_df = resolved_df[cols].copy()

    # 2. Convert is_resolved to bool if it exists
    if "is_resolved" in sink_df.columns:
        sink_df["is_resolved"] = sink_df["is_resolved"].astype(bool)

    # 3. Convert to Arrow table
    arrow_table = pa.Table.from_pandas(sink_df, preserve_index=False)

    # 4. Create SQL catalog backed by SQLite (metadata only)
    #    Actual data + metadata files go to GCS warehouse
    catalog = SqlCatalog(
        "corp_catalog",
        **{
            "uri":        CATALOG_URI,
            "warehouse":  ICEBERG_WAREHOUSE,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )

    # 5. Create namespace
    namespace = "corp"
    try:
        catalog.create_namespace(namespace)
        print(f"Created Iceberg namespace: {namespace}")
    except NamespaceAlreadyExistsError:
        print(f"Namespace '{namespace}' already exists — continuing.")

    # 6. Define schema
    schema = Schema(
        NestedField(1, "corporate_name", StringType(),  required=False),
        NestedField(2, "canonical_id",   StringType(),  required=False),
        NestedField(3, "is_resolved",    BooleanType(), required=False),
    )

    # 7. Create or load table
    identifier = f"{namespace}.corporate_registry"
    try:
        table = catalog.create_table(identifier=identifier, schema=schema)
        print(f"Created Iceberg table: {identifier}")
    except TableAlreadyExistsError:
        table = catalog.load_table(identifier)
        print(f"Loaded existing Iceberg table: {identifier}")

    # 8. Append data
    table.append(arrow_table)
    print(f"Successfully committed {len(sink_df)} records to Iceberg table.")
    print(f"Table location: {ICEBERG_WAREHOUSE}/{namespace}/corporate_registry/")


# ──────────────────────────────────────────────
# Pipeline entrypoint
# ──────────────────────────────────────────────
def run_pipeline() -> None:
    # KEY FIX: do not pass "auto" — let Ray use RAY_ADDRESS env var
    # which KubeRay injects automatically into the submitter pod
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    print("=" * 50)
    print("Pipeline Execution Started.")
    print("=" * 50)

    # Parallel ingestion
    future_a = ingest_data.remote("SUPPLYCHAIN_DATA_S1")
    future_b = ingest_data.remote("FINANCIAL_DATA_S2")

    data_sources = ray.get([future_a, future_b])
    combined_df  = pd.concat(data_sources, ignore_index=True)
    print(f"Ingested total of {len(combined_df)} records.")

    # Entity resolution on worker node
    print("Starting LLM-based Entity Resolution...")
    resolver    = EntityResolver.remote()
    resolved_df = ray.get(resolver.resolve_batch.remote(combined_df))

    # Summary
    print("--- Harmonized Data Summary ---")
    print(resolved_df[["corporate_name", "canonical_id", "is_resolved"]].head(10).to_string())

    # Iceberg write
    write_to_iceberg(resolved_df)

    print("=" * 50)
    print("Pipeline Execution Complete.")
    print("=" * 50)


if __name__ == "__main__":
    run_pipeline()