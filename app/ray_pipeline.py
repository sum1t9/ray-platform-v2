import os
import time

import pandas as pd
import ray

# JM Comment: Ray pipeline v2 for entity resolution and Iceberg sink


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


@ray.remote(num_gpus=0, resources={"entity_resolver": 1})
class EntityResolver:
    """Simulated LLM-based Entity Resolution actor."""

    def __init__(self):
        self.api_key = os.getenv("LLM_API_KEY", "NOT_SET")
        if self.api_key == "NOT_SET":
            print(
                "CRITICAL WARNING: LLM_API_KEY not found in environment. "
                "Resolution will be a no-op."
            )

    def resolve_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        print(f"--- [Actor] Processing batch of {len(df)} records via simulated LLM ---")
        time.sleep(3)
        df["canonical_id"] = df["corporate_name"].apply(
            lambda x: f"UID-{hash(x) % 10000}"
        )
        df["is_resolved"] = True
        return df


def write_to_iceberg(resolved_df: pd.DataFrame) -> None:
    """Placeholder for writing to Apache Iceberg."""
    print("--- [Sink] Writing to Apache Iceberg table corporate_registry ---")
    # from pyiceberg.catalog import load_catalog
    # catalog = load_catalog("default")
    # table = catalog.load_table("corporate_registry")
    time.sleep(1)
    print("Successfully committed transaction to Iceberg table.")


def run_pipeline() -> None:
    if not ray.is_initialized():
        ray.init("auto")

    print("Pipeline Execution Started.")

    future_a = ingest_data.remote("SUPPLYCHAIN_DATA_S1")
    future_b = ingest_data.remote("FINANCIAL_DATA_S2")

    data_sources = ray.get([future_a, future_b])
    combined_df = pd.concat(data_sources)
    print(f"Ingested total of {len(combined_df)} records.")

    resolver = EntityResolver.remote()
    print("Starting LLM-based Entity Resolution...")
    resolved_df = ray.get(resolver.resolve_batch.remote(combined_df))

    print("--- Harmonized Data Summary ---")
    print(resolved_df[["corporate_name", "canonical_id", "is_resolved"]].head())

    write_to_iceberg(resolved_df)


if __name__ == "__main__":
    run_pipeline()