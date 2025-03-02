import altair as alt
import polars as pl
from glob import glob

f = max(glob("data-saved/ingest/*.csv"))
print(f)
df = pl.read_csv(f).with_columns(
    (pl.col("utilized_capacity") / pl.col("total_capacity")).alias(
        "capacity utilization"
    ),
    (pl.col("total_capacity") / pl.col("num_node")).alias("average node capacity"),
)
(
    alt.Chart(df.filter(pl.col("capacity_skew") == 1.5))
    .mark_point()
    .encode(
        alt.X("average node capacity").scale(type="log"),
        alt.Y("capacity utilization"),
        alt.Color("strategy"),
    )
    | alt.Chart(df.filter(pl.col("node_min_capacity") == 1 << 12))
    .mark_boxplot()
    .encode(
        alt.X("capacity_skew").scale(zero=False),
        alt.Y("capacity utilization"),
        alt.Color("strategy"),
    )
).resolve_scale(y="shared").save("ingest.png")
