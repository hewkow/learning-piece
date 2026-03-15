import polars as pl
import numpy as np
import bottleneck as bn
from numba import njit

@njit
def generate_mockup_market(bars: int = 365) -> np.ndarray:
    np.random.seed(36)
    s0 = 100.0
    mu = 1
    sigma = 1
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



close, entries, exits = generate_close_entries_exits(365*5)
df = pl.DataFrame({"close": close, "entries": entries, "exits": exits})
buy_size = 5_000
init_cash = 10_000

# 1.point when the entry and exit happen with the value of current close 
df = df.with_columns(
    cash = pl.lit(init_cash),
    entry_value = pl.when(pl.col("entries")).then(pl.col("close") ).otherwise(None),
    exit_value = pl.when(pl.col("exits")).then(pl.col("close") ).otherwise(None),
# 2. detect is current open position
    hold = (pl.col('entries').cast(pl.Int8) - pl.col('exits').cast(pl.Int8)).cum_sum().clip(0, 1),
# 3. seperate each trade by id    
    trade_id = pl.col('entries').cum_sum()
).with_columns(
# 4. create columns that cumulative of buy_size because I design to use 3 together 1.cash 2.cash_spend 3. cash_earn to create a real cash flow
    cash_spend = pl.when(pl.col('entries')).then( buy_size ).otherwise(0).cum_sum()
)


# 5. small df that pull only when entry and exit price to calculate realize pnl and whole position pnl 
realize_df =  (df.group_by('trade_id').agg(
    pl.col('entry_value').first(ignore_nulls=True),
    pl.col('exit_value').first(ignore_nulls=True),
    )
    .sort('trade_id')
    .with_columns(
        realize =( pl.col('exit_value') - pl.col('entry_value') ) * (buy_size / pl.col('entry_value') ),
        hold_pos = ( pl.col('exit_value') *  (buy_size / pl.col('entry_value') ) ),
    )
)

print(realize_df)

# 6. forward fill first from only one point per entry
df = df.with_columns(
    pl.col('entry_value').forward_fill()
# 7. now just filter to only when the position is open
).with_columns(
    entry_value = pl.when(pl.col('hold') == 1).then(pl.col('entry_value')).otherwise(0)
# 8. so i can get clean unrealize pnl that not appear all the time, appear only position is open
).with_columns(
    unrealize = pl.when(pl.col('entry_value') > 0).then(pl.col('close') * (buy_size / pl.col('entry_value')))
)
# 9. join the small df to get realize pnl and position pnl
df = df.join(realize_df,on='exit_value', how='full')

# 10. this is when calulate after realize pnl
df = df.with_columns(
    cash_earn = pl.col('hold_pos').cum_sum().forward_fill()

).with_columns(
    cash_real = pl.col('cash') + pl.col('cash_earn').fill_null(0.0) - pl.col('cash_spend').fill_null(0.0)
).with_columns(
    equity = pl.col('cash_real') + pl.col('unrealize').fill_null(0.0)
)

print(df.schema)

import plotly.graph_objects as go
fig_eq = go.Figure()
fig_eq.add_trace(go.Scatter(y=df['cash_earn'],mode='lines',name='cash earn'))
fig_eq.add_trace(go.Scatter(y=df['cash_spend'],mode='lines',name='cash spend'))
fig_eq.add_trace(go.Scatter(y=df['equity'],mode='lines',name='equity'))
fig_eq.add_trace(go.Scatter(y=df['cash_real'],mode='lines',name='cash real'))
fig_eq.add_trace(go.Scatter(y=df['close'],mode='lines',name='close'))

fig_eq.show()