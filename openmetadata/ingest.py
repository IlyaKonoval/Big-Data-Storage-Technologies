"""OpenMetadata ingestion for the CelesTrak platform (service om-ingest).

    python ingest.py once   run every workflow of workflows/*.yaml, then publish lineage, the
                            curated metadata of catalog.yaml, run the data quality tests
                            (quality.py) and refresh Data Insights (the KPIs)
    python ingest.py loop   the same every OM_INGEST_INTERVAL seconds (default 1 h)

The workflows use ${VAR} placeholders: connection settings come from the environment, the
JWT of ingestion-bot (OM_JWT) is fetched from the server with the admin login.
"""
import logging
import os
import sys
import time
from pathlib import Path

import yaml
from metadata.workflow.metadata import MetadataWorkflow

import catalog
import quality
from celestrak import lineage

# The metadata package reconfigures the root logger; our loggers get their own handler.
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
for _name in ("om-ingest", "om-catalog", "om-quality", "celestrak"):
    logging.getLogger(_name).addHandler(_handler)
    logging.getLogger(_name).setLevel(logging.INFO)
    logging.getLogger(_name).propagate = False
log = logging.getLogger("om-ingest")

WORKFLOWS = sorted((Path(__file__).parent / "workflows").glob("*.yaml"))
INTERVAL = int(os.environ.get("OM_INGEST_INTERVAL", "3600"))


def run_workflow(path: Path) -> bool:
    config = yaml.safe_load(os.path.expandvars(path.read_text(encoding="utf-8")))
    workflow = MetadataWorkflow.create(config)
    try:
        workflow.execute()
        workflow.raise_from_status()
        return True
    except Exception:
        log.exception("Workflow %s failed", path.name)
        return False
    finally:
        workflow.print_status()
        workflow.stop()


def run_once() -> bool:
    om = lineage.Client()
    os.environ["OM_JWT"] = om.bot_token()
    ok = all([run_workflow(path) for path in WORKFLOWS])
    result = lineage.publish(om)
    log.info("Lineage: %s", result)
    log.info("Catalog: %s", catalog.apply(om))
    tests = quality.run(om, catalog.tables())
    # KPIs are computed by the Data Insights app (daily by default): refresh them now.
    try:
        om.request("POST", "/v1/apps/trigger/DataInsightsApplication", json={})
    except RuntimeError as e:   # e.g. still running from the previous round
        log.warning("Data Insights not triggered: %s", e)
    return ok and not result["missing"] and not tests["suite_errors"]


def main(mode: str) -> int:
    if mode == "once":
        return 0 if run_once() else 1
    while True:
        try:
            run_once()
        except Exception:
            log.exception("Ingestion round failed")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "once"))
