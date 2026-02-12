from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    catalog: str = os.getenv("RB_CATALOG", "main")
    bronze_schema: str = os.getenv("RB_BRONZE_SCHEMA", "realbricks_bronze")
    silver_schema: str = os.getenv("RB_SILVER_SCHEMA", "realbricks_silver")
    gold_schema: str = os.getenv("RB_GOLD_SCHEMA", "realbricks_gold")
    ops_schema: str = os.getenv("RB_OPS_SCHEMA", "realbricks_ops")
    max_rows_per_source: int = int(os.getenv("RB_MAX_ROWS_PER_SOURCE", "0"))
    fail_on_missing_source: bool = os.getenv("RB_FAIL_ON_MISSING_SOURCE", "false").lower() == "true"
    min_listing_rows: int = int(os.getenv("RB_MIN_LISTING_ROWS", "1"))
    min_non_null_ratio: float = float(os.getenv("RB_MIN_NON_NULL_RATIO", "0.50"))
    state_backend: str = os.getenv("RB_STATE_BACKEND", "delta")
    lakebase_dsn: str = os.getenv("RB_LAKEBASE_DSN", "")
    lakebase_table: str = os.getenv("RB_LAKEBASE_TABLE", "deal_state")

    @property
    def bronze_prefix(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}"

    @property
    def silver_prefix(self) -> str:
        return f"{self.catalog}.{self.silver_schema}"

    @property
    def gold_prefix(self) -> str:
        return f"{self.catalog}.{self.gold_schema}"

    @property
    def ops_prefix(self) -> str:
        return f"{self.catalog}.{self.ops_schema}"

    @property
    def silver_listing_table(self) -> str:
        return f"{self.silver_prefix}.listing_core"

    @property
    def gold_property_rank_table(self) -> str:
        return f"{self.gold_prefix}.property_rankings"

    @property
    def deal_state_table(self) -> str:
        return f"{self.ops_prefix}.deal_state"

    @property
    def run_history_table(self) -> str:
        return f"{self.ops_prefix}.run_history"

    @property
    def source_history_table(self) -> str:
        return f"{self.ops_prefix}.source_history"


def get_settings() -> Settings:
    return Settings()

