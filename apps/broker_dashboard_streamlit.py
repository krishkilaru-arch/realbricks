from __future__ import annotations

import os
from datetime import datetime, timezone

try:
    import streamlit as st
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "Streamlit is required for app runtime. Install it in Databricks App environment."
    ) from exc

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession

from realbricks.app_config import load_app_config
from realbricks.state_store import DeltaStateStore


def _current_user() -> str:
    # Databricks Apps commonly provide user identity through env or platform context.
    candidates = (
        os.getenv("RB_APP_USER"),
        os.getenv("DATABRICKS_USERNAME"),
        os.getenv("DATABRICKS_USER"),
        os.getenv("USER"),
    )
    for item in candidates:
        if item and item.strip():
            return item.strip()
    return "unknown"


def _is_authorized(user: str, users: list[str], domains: list[str]) -> bool:
    lowered = user.lower()
    if users and lowered in users:
        return True
    if domains and "@" in lowered:
        domain = lowered.split("@", 1)[1]
        if domain in domains:
            return True
    return not users and not domains


def _safe_page_size(default_size: int, max_size: int) -> int:
    size = st.sidebar.number_input(
        "Page size",
        min_value=1,
        max_value=max_size,
        value=default_size,
        step=1,
    )
    return int(size)


def _paginate(df, order_col: str, page: int, page_size: int, descending: bool = True):
    window = Window.orderBy(F.col(order_col).desc_nulls_last() if descending else F.col(order_col).asc_nulls_last())
    start = (page - 1) * page_size + 1
    end = page * page_size
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .where((F.col("_rn") >= start) & (F.col("_rn") <= end))
        .drop("_rn")
    )


st.set_page_config(page_title="RealBricks Broker Dashboard", layout="wide")

spark = SparkSession.builder.getOrCreate()
config_path = os.getenv("RB_APP_CONFIG_PATH", "conf/app_config.json")

try:
    cfg = load_app_config(config_path)
except Exception as exc:
    st.error(str(exc))
    st.stop()

user = _current_user()
authorized = _is_authorized(user, cfg.allowed_users, cfg.allowed_email_domains)
if not authorized:
    st.error(f"User `{user}` is not authorized for this app.")
    st.stop()

st.title(cfg.app_title)
st.caption("Lead-to-Offer accelerator operational view")
st.sidebar.success(f"Signed in as: {user}")
page_size = _safe_page_size(cfg.page_size_default, cfg.page_size_max)

left, right = st.columns(2)

with left:
    st.subheader("Market KPI Snapshot")
    try:
        zip_filter = st.text_input("Filter ZIP (optional)", value="")
        page = int(st.number_input("KPI page", min_value=1, value=1, step=1))
        kpi_df = spark.table(cfg.kpi_table)
        if zip_filter.strip():
            kpi_df = kpi_df.where(F.col("zip") == zip_filter.strip())
        total = kpi_df.count()
        kpi_df = _paginate(kpi_df, order_col="listing_count", page=page, page_size=page_size)
        st.caption(f"Rows: {total} | Page: {page}")
        st.dataframe(kpi_df, use_container_width=True)
    except Exception as exc:
        st.error(f"Unable to load KPI table `{cfg.kpi_table}`: {exc}")

with right:
    st.subheader("Top Ranked Properties")
    try:
        min_price = st.number_input("Min price", min_value=0, value=0, step=10000)
        max_price = st.number_input("Max price", min_value=0, value=2000000, step=10000)
        rank_page = int(st.number_input("Rank page", min_value=1, value=1, step=1))
        rank_df = spark.table(cfg.ranking_table)
        rank_df = rank_df.where((F.col("list_price") >= float(min_price)) & (F.col("list_price") <= float(max_price)))
        total_rank = rank_df.count()
        rank_rows = _paginate(rank_df, order_col="ranking_score", page=rank_page, page_size=page_size)
        st.caption(f"Rows: {total_rank} | Page: {rank_page}")
        st.dataframe(rank_rows, use_container_width=True)
    except Exception as exc:
        st.error(f"Unable to load ranking table `{cfg.ranking_table}`: {exc}")

st.subheader("Deal State Timeline")
try:
    deal_id = st.text_input("Deal ID filter", value="")
    state_page = int(st.number_input("State page", min_value=1, value=1, step=1))
    state_df = spark.table(cfg.state_table)
    if deal_id:
        state_df = state_df.where(F.col("deal_id") == deal_id)
    total_state = state_df.count()
    state_rows = _paginate(state_df, order_col="event_time", page=state_page, page_size=page_size)
    st.caption(f"Rows: {total_state} | Page: {state_page}")
    st.dataframe(state_rows, use_container_width=True)
except Exception as exc:
    st.error(f"Unable to load deal state table `{cfg.state_table}`: {exc}")

if cfg.enable_actions:
    st.subheader("Broker Actions")
    with st.form("broker_action_form"):
        action_deal_id = st.text_input("Deal ID", value=deal_id or "")
        action = st.selectbox(
            "Action",
            options=["approve", "needs_review", "escalate", "request_docs", "reject"],
        )
        notes = st.text_area("Notes", value="")
        submitted = st.form_submit_button("Submit Action")

    if submitted:
        if not action_deal_id.strip():
            st.error("Deal ID is required.")
        else:
            try:
                store = DeltaStateStore(spark=spark, table_name=cfg.state_table)
                store.write_event(
                    deal_id=action_deal_id.strip(),
                    stage="broker_action",
                    payload={
                        "action": action,
                        "notes": notes,
                        "actor": user,
                        "submitted_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                st.success(f"Action `{action}` recorded for deal `{action_deal_id}`.")
            except Exception as exc:
                st.error(f"Failed to record action: {exc}")

