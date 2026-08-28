"""Central configuration: naming conventions, fully-qualified table names,
volume paths. Everything is derived from the `catalog` job parameter so the
same code runs unchanged in dev / uat / prod.
"""

from dataclasses import dataclass, field


SCHEMAS = ["landing", "bronze", "silver", "gold", "ops"]

# Public TLC endpoints (CloudFront distribution used by nyc.gov TLC page)
TLC_TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
TLC_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


@dataclass
class Config:
    """Environment-aware configuration object.

    Usage:
        cfg = Config(catalog="taxi_dev", env="dev")
        cfg.bronze_trips          -> "taxi_dev.bronze.yellow_trips_raw"
        cfg.landing_trips_path    -> "/Volumes/taxi_dev/landing/raw/yellow"
    """

    catalog: str
    env: str = "dev"
    run_month: str = ""          # YYYY-MM being processed (informational)
    full_refresh: bool = False

    # populated in __post_init__
    tables: dict = field(default_factory=dict, init=False)

    # ---- volumes -----------------------------------------------------------
    @property
    def landing_volume(self) -> str:
        return f"/Volumes/{self.catalog}/landing/raw"

    @property
    def landing_trips_path(self) -> str:
        return f"{self.landing_volume}/yellow"

    @property
    def landing_zones_path(self) -> str:
        return f"{self.landing_volume}/zones"

    @property
    def checkpoints_volume(self) -> str:
        return f"/Volumes/{self.catalog}/ops/checkpoints"

    # ---- bronze ------------------------------------------------------------
    @property
    def bronze_trips(self) -> str:
        return f"{self.catalog}.bronze.yellow_trips_raw"

    @property
    def bronze_zones(self) -> str:
        return f"{self.catalog}.bronze.taxi_zone_lookup"

    # ---- silver ------------------------------------------------------------
    @property
    def silver_trips(self) -> str:
        return f"{self.catalog}.silver.yellow_trips"

    @property
    def silver_quarantine(self) -> str:
        return f"{self.catalog}.silver.yellow_trips_quarantine"

    # ---- gold --------------------------------------------------------------
    @property
    def dim_date(self) -> str:
        return f"{self.catalog}.gold.dim_date"

    @property
    def dim_zone(self) -> str:
        return f"{self.catalog}.gold.dim_zone"

    @property
    def dim_vendor(self) -> str:
        return f"{self.catalog}.gold.dim_vendor"

    @property
    def dim_payment_type(self) -> str:
        return f"{self.catalog}.gold.dim_payment_type"

    @property
    def dim_rate_code(self) -> str:
        return f"{self.catalog}.gold.dim_rate_code"

    @property
    def fact_trips(self) -> str:
        return f"{self.catalog}.gold.fact_trips"

    @property
    def agg_daily_zone(self) -> str:
        return f"{self.catalog}.gold.agg_daily_zone_revenue"

    @property
    def agg_monthly_kpis(self) -> str:
        return f"{self.catalog}.gold.agg_monthly_kpis"

    # ---- ops ---------------------------------------------------------------
    @property
    def ops_pipeline_runs(self) -> str:
        return f"{self.catalog}.ops.pipeline_runs"

    @property
    def ops_dq_results(self) -> str:
        return f"{self.catalog}.ops.dq_results"

    @property
    def ops_recon_results(self) -> str:
        return f"{self.catalog}.ops.recon_results"

    @property
    def ops_ingest_manifest(self) -> str:
        return f"{self.catalog}.ops.ingest_manifest"


def config_from_widgets(dbutils) -> Config:
    """Build Config from standard job/notebook widgets."""
    catalog = dbutils.widgets.get("catalog")
    env = dbutils.widgets.get("env")
    try:
        run_month = dbutils.widgets.get("run_month")
    except Exception:
        run_month = ""
    try:
        full_refresh = dbutils.widgets.get("full_refresh").lower() == "true"
    except Exception:
        full_refresh = False
    return Config(catalog=catalog, env=env, run_month=run_month, full_refresh=full_refresh)
