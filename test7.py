import polars as pl
from datetime import datetime


df = pl.read_json("dashboard_dataframe_multimodel.json")
# applying an exclusion modifies the df
df = df.filter(~pl.col("date").is_between(datetime(2024, 1, 10), datetime(2024, 1, 25)), pl.col("model") == "v3.0.0")

window = df.filter((pl.col("date").is_between(datetime(2024, 1, 8), datetime(2024, 2, 2))))

summary = df.group_by("model").agg(pl.col("*").sum())\
    .with_columns(
        (pl.col("profit_total")/pl.col("mwh_total")).alias("per MWh"),
        (100*pl.col("win_count")/pl.col("mwh_total")).alias("win %")
    )\
    .filter(pl.col("model") == "v3.0.0")\
    .select(
        pl.col("profit_total").alias("PnL"),
        "per MWh",
        "win %"
    )
