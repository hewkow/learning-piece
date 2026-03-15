import polars as pl
import numpy as np
import bottleneck as bn
from numba import njit

@njit
def generate_mockup_market(bars: int = 365) -> np.ndarray:
    np.random.seed(36)
    s0 = 100.0
    mu = 0.1
    sigma = 0.1
    days = bars
    dt = 1/days
    
    daily_returns = np.random.normal(
        (mu - 0.5 * sigma**2) * dt,
        sigma * np.sqrt(dt),
        size =days
    )
    
    return s0 * np.exp(np.cumsum(daily_returns))

def generate_close_entries_exits(bars: int = 1_000_000) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    close = generate_mockup_market(bars)
    sma20 = bn.move_mean(close, 20)
    sma50 = bn.move_mean(close, 50)
    
    condition_ = sma20 > sma50
    
    entries = condition_ & np.roll(~condition_, 1)
    entries[0] = False
    exits = ~condition_ & np.roll(condition_, 1)
    exits[0] = False
    return close, entries, exits



close, entries, exits = generate_close_entries_exits(365)
df = pl.DataFrame({"close": close, "entries": entries, "exits": exits})
buy_size = 1_000
init_cash = 10_000
df = df.with_columns(
    cash = pl.lit(init_cash),
    position = pl.lit(0),
    entry_value = pl.lit(0),
    exit_value = pl.lit(0),
    hold = pl.lit(0),
)
df = df.with_columns(
    position = pl.when(pl.col("entries")).then(1).otherwise(0),
    entry_value = pl.when(pl.col("entries")).then(pl.col("close") ).otherwise(None),
    exit_value = pl.when(pl.col("exits")).then(pl.col("close") ).otherwise(None),
    hold = (pl.col('entries').cast(pl.Int8) - pl.col('exits').cast(pl.Int8)).cum_sum().clip(0, 1),
    trade_id = pl.col('entries').cum_sum()
)

df = df.with_columns(
    entry_value = pl.col('entry_value').forward_fill(),
    exit_value = pl.col('exit_value').forward_fill()
).with_columns( # need forward_fill() to apply first, that why we have to put another with_columns()
    realize = (pl.col('close') * ( buy_size / pl.col('entry_value') ))
).with_columns(
    bought = pl.when('entries').then(buy_size).otherwise(0).cum_sum()
).with_columns(
    realize_cum = pl.when('exits').then(pl.col('realize')).otherwise(0).cum_sum()
).with_columns(
    equity_after_sell = (pl.col('cash') + pl.col('realize_cum') )- pl.col('bought')
)

import plotly.graph_objects as go
fig_eq = go.Figure()
fig_eq.add_trace(go.Scatter(y=df['realize_cum'],mode='lines+markers',name='realize'))
fig_eq.add_trace(go.Scatter(y=df['bought'],mode='lines+markers',name='cash use to bought'))
fig_eq.add_trace(go.Scatter(y=df['equity_after_sell'],mode='lines+markers',name='equity'))
fig_eq.show()