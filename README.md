# RealBricks (Databricks Summit Short Version)

This is a shorter version prepared for Databricks Summit submission.

It demonstrates an end-to-end Lead-to-Offer accelerator on Databricks with:

- Multi-agent orchestration (Lead, Property Match, Mortgage, Document/Offer)
- Config-driven Delta Sharing ingestion and Bronze/Silver/Gold modeling
- Stateful workflow checkpoints with Delta/Lakebase backend options
- Production-oriented workflows via Databricks Asset Bundles
- Broker app scaffold for operational visibility and human review actions

## Current Status

The implementation is evolving and production-oriented.

If selected, we can demo the full end-to-end workflow:

1. Ingest sample/marketplace data
2. Build canonical models and ranking outputs
3. Run supervised deal orchestration across phases
4. Review outputs in broker-facing app and workflow state timeline

## Quick Start

```bash
python -m pip install -e .
python -m realbricks.main phase --phase 1 --config conf/phases/phase1.json --deal-id summit-demo
```

## Workflow Deployment

```bash
databricks bundle validate --var "existing_cluster_id=<your-cluster-id>"
databricks bundle deploy --var "existing_cluster_id=<your-cluster-id>"
databricks bundle run realbricks_phase_pipeline --var "existing_cluster_id=<your-cluster-id>"
```

## Notes

- Fill runtime config in `conf/app_config.json` from `conf/app_config.template.json`.
- Keep credentials in secrets only (never commit secret files).

