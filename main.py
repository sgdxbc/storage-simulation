import altair as alt
import polars as pl
from glob import glob

df = pl.read_csv(glob("data-saved/*.csv")[-1]).with_columns(
    (pl.col("utilized_capacity") / pl.col("total_capacity")).alias(
        "capacity utilization"
    )
)
df
box_chart = (
    alt.Chart(df)
    .encode(
        alt.X("node_min_capacity").scale(zero=False),
        alt.Y("capacity utilization"),
        color="strategy",
    )
    .mark_boxplot()
)
line_chart = (
    alt.Chart(df)
    .encode(x="node_min_capacity", y="mean(capacity utilization)", color="strategy")
    .mark_line()
)
(box_chart + line_chart).save("graph.png")