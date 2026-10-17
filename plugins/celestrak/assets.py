"""Airflow Assets that chain the layers: ods_<source> -> vault_<name> -> vault_business.

A DAG lists an asset in its task outlets when it has written fresh data, and downstream
DAGs use the asset as their schedule (data-aware scheduling instead of sensors).
"""
from airflow.sdk import Asset


def asset(layer: str, name: str) -> Asset:
    return Asset(name=f"{layer}_{name}", uri=f"celestrak://{layer}/{name}")
