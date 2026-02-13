# RealBricks - Databricks Summit Edition

RealBricks is a Databricks-native, multi-agent accelerator for residential real estate workflows.  
This repository is the concise Summit version and is actively evolving.

## Why This Matters Now

Imagine a broker receives a high-intent buyer lead at 9:00 AM.

- By 9:15 AM, the lead is sitting in CRM but not fully qualified.
- By 10:00 AM, listing data is pulled from a different system and manually compared.
- By 11:30 AM, financing details are still unclear because mortgage signals are disconnected.
- By afternoon, offer documents are reviewed manually and key risks are easy to miss.
- By the time the broker responds with confidence, the buyer has already moved to another agent.

This is not a technology problem in isolation. It is an orchestration problem:

- too many systems,
- too many handoffs,
- too much latency between decisions.

RealBricks exists to compress that timeline from days to under an hour by combining:

- unified data flows on Databricks,
- coordinated AI agents for each decision step,
- stateful workflow memory for resumability and auditability,
- a broker-facing operational view for human-in-the-loop control.

The goal is simple: help brokers spend less time stitching systems together and more time closing the right deals faster.

If selected, we can demo an end-to-end lead-to-offer flow:

1. ingest marketplace/sample data,
2. build canonical models,
3. run supervisor-led agent orchestration,
4. review outcomes in the broker app and workflow timeline.

## Business Challenge We Solve

Typical residential workflows are fragmented across CRM, listings, financing, and document systems.  
RealBricks provides one orchestrated pipeline to:

- score and qualify leads,
- match/rank properties,
- assess affordability and financing risk,
- parse offer terms and risks,
- keep stateful, auditable workflow history.

## Solution Capabilities

- Multi-agent orchestration:
  - Lead Qualification
  - Property Matching
  - Mortgage/Financing
  - Document/Offer
  - Supervisor
- Data engineering:
  - config-driven ingestion
  - Bronze/Silver/Gold modeling
  - quality gates
- Stateful workflow memory:
  - Delta-backed state
  - optional Lakebase-backed state
- Operational reliability:
  - run/source audit tables
  - strict mode for missing sources
  - workflow bundle deployment
- Broker-facing app:
  - KPI/ranking/deal timeline views
  - action write-back for human-in-the-loop decisions

## Reference Architecture

- Ingestion: Delta Sharing/Marketplace sources -> Bronze tables
- Modeling: Bronze -> Silver canonical listing model -> Gold rankings + market KPIs
- Orchestration: Supervisor calls sub-agents and checkpoints every stage
- State: Delta (default) or Lakebase (production option)
- UI: Streamlit app for broker operations
- Deploy: Databricks Asset Bundle workflows

## Codebase Layout

- `src/realbricks/main.py` - CLI entrypoint
- `src/realbricks/agents/` - agent logic
- `src/realbricks/pipelines/` - ingestion and models
- `src/realbricks/orchestration/` - supervisor and phase runners
- `src/realbricks/state_store.py` - Delta/Lakebase state backends
- `src/realbricks/connectors/` - external API connector scaffolds
- `apps/broker_dashboard_streamlit.py` - broker app
- `conf/phases/` - phase-specific source configs
- `resources/workflows.yml` - Databricks jobs definitions
- `databricks.yml` - Databricks Asset Bundle config
- `tests/` - unit tests

## Implementation Roadmap

- Phase 1: supervisor + lead/property/mortgage agents + core data flow
- Phase 2: document/offer extraction + what-if affordability scenarios
- Phase 3: production integration baseline + connector health checks
- Phase 4: multi-deal scale simulation + readiness summary metrics

## Prerequisites and Access

- Python 3.10+
- Databricks workspace access
- Existing Databricks cluster ID for jobs/workflows
- Marketplace/Delta Sharing access to configured catalogs

Optional for advanced mode:

- `streamlit` for app runtime
- `psycopg` for Lakebase backend

## Setup

```bash
python -m pip install -e .
```

Optional extras:

```bash
python -m pip install -e ".[app]"
python -m pip install -e ".[lakebase]"
```

## Configuration Guide

### 1) Phase/Data Configs

Phase configs are pre-created in:

- `conf/phases/phase1.json`
- `conf/phases/phase2.json`
- `conf/phases/phase3.json`
- `conf/phases/phase4.json`

Update source table names if your workspace differs.

### 2) App Config (you fill this)

Create:

- `conf/app_config.json` from `conf/app_config.template.json`

This file controls:

- table names,
- page limits,
- access control (allowed users/domains),
- action enablement.

### 3) Runtime Environment Variables

- `RB_CATALOG` (default: `main`)
- `RB_BRONZE_SCHEMA` (default: `realbricks_bronze`)
- `RB_SILVER_SCHEMA` (default: `realbricks_silver`)
- `RB_GOLD_SCHEMA` (default: `realbricks_gold`)
- `RB_OPS_SCHEMA` (default: `realbricks_ops`)
- `RB_MAX_ROWS_PER_SOURCE` (default: `0`)
- `RB_FAIL_ON_MISSING_SOURCE` (default: `false`)
- `RB_MIN_LISTING_ROWS` (default: `1`)
- `RB_MIN_NON_NULL_RATIO` (default: `0.50`)

State backend selection:

- `RB_STATE_BACKEND` = `delta` (default) or `lakebase`
- `RB_LAKEBASE_DSN` (required when `RB_STATE_BACKEND=lakebase`)
- `RB_LAKEBASE_TABLE` (default: `deal_state`)

App:

- `RB_APP_CONFIG_PATH` (default: `conf/app_config.json`)

## Local and CLI Execution

Run ingestion only:

```bash
python -m realbricks.main ingest --config conf/phases/phase1.json
```

Run models only:

```bash
python -m realbricks.main models --config conf/phases/phase1.json
```

Run one deal orchestration:

```bash
python -m realbricks.main deal --deal-id demo-001 --lead-json '{"lead_id":"L-100","budget":650000,"target_zip":"75205","household_income":180000,"preapproved":true,"intent_days":21,"min_beds":3,"down_payment":120000,"interest_rate":0.0675}'
```

Run a full phase:

```bash
python -m realbricks.main phase --phase 1 --config conf/phases/phase1.json --deal-id summit-demo
```

## Databricks Workflow Orchestration

Validate:

```bash
databricks bundle validate --var "existing_cluster_id=<your-cluster-id>"
```

Deploy:

```bash
databricks bundle deploy --var "existing_cluster_id=<your-cluster-id>"
```

Run full pipeline job:

```bash
databricks bundle run realbricks_phase_pipeline --var "existing_cluster_id=<your-cluster-id>"
```

Run daily refresh workflow:

```bash
databricks bundle run realbricks_refresh_phase1 --var "existing_cluster_id=<your-cluster-id>"
```

## Broker Experience App

Run app (environment with Spark + Streamlit):

```bash
streamlit run apps/broker_dashboard_streamlit.py
```

App features:

- KPI and ranked property pages with pagination/filters
- deal state timeline viewer
- broker action write-back (`broker_action` stage events)
- access control via app config allow-lists

## Lakebase: Production State Strategy

Use Delta backend for portability and fast startup.  
Use Lakebase backend for production-grade mutable, resumable workflow state:

- better state transition handling,
- safer resume/recovery behavior,
- clean separation between analytical tables and operational state memory.

## Production Readiness Highlights

- run-level and source-level observability tables
- ingestion config validation
- quality gates before model serving
- supervisor error checkpointing
- unit test coverage for core logic/config
- configurable strict mode for source integrity

## Current Scope (Summit Version)

- This repo is a concise implementation baseline, not the final enterprise package.
- External API connectors are scaffolded and require real credentials/endpoints.
- Some advanced policy-as-code and full vector/geospatial optimization are roadmap items.

## Security and Secret Management

- Never commit secrets.
- Keep local secret/config files out of Git:
  - `.secrets.local`
  - `conf/app_config.json`
- Use Databricks secret scopes for credentials.

## Roadmap and Next Steps

This implementation is evolving.  
If selected, we can demonstrate and extend the full lead-to-offer lifecycle with production integrations and a richer broker app experience.

