# The backtest pure Polars

## [[1.py]]

* At this first stage, I still tended to think in mutable row-by-row state.
* The first thing that pressured me to change the way I think was `trade_id`. I had already added it to the DataFrame, but I still did not know how to really use it.
* So at this stage I was still trying to calculate as much as possible row by row, while many parts were still opaque, especially cash flow and how equity should actually be resolved.
* I also used `forward_fill()`, but at that time I did not fully realize its meaning. I only knew I needed it so later rows could use values that only existed at entry or exit points.

## [[2.py]]

* I got hints to try `group_by()`, and at this stage I started to see that I could move some calculations into another DataFrame that only exists at trade level.
* This was the point where I began to see that not everything has to live in the main bar-by-bar DataFrame.
* But even then, I still did not fully know how to use that grouped trade information in the full backtest flow.

## [[experiment.py]]

* At this stage, I started to realize that I could calculate actual realized results in another DataFrame created from `group_by()`, so I called it `realize_df`.
* Then I joined it back into the main DataFrame.
* The other big problem was still the opaque part around cash and equity, so my solution was to separate them into:

  * cash spend (`buy_size`)
  * cash earn (`realized pnl`)
  * cash start
* Then I calculated `cash_real` as the actual cash flow moving in and out.
* That was the point where I could finally calculate equity in a way that felt real instead of opaque.

# What I have learned

* At first I thought I had to do everything in one transform, but it is actually more like a staged pipeline where one derived column enables the next one.
* Even if it is vectorized, that does not mean it has no sequence. The sequence just moves from row-by-row execution to sequence of transformation phases.
* I learned to perceive trades as event markers and boundaries, not only as row-to-row mutation.
* I learned that holding state can be derived from entry and exit events instead of being manually updated row by row.
* I learned that `forward_fill()` is important because event values are sparse at first, and later calculations need those values to become continuous context.
* I learned that some calculations belong to the main bar-level DataFrame, but some belong more naturally to a trade-level DataFrame.
* I learned that `group_by()` lets me summarize trade-level information, and `join()` lets me bring that information back to the main timeline.
* I learned that cash flow is easier to understand when I separate it into components instead of trying to calculate everything as one opaque number.
* I learned that equity became clearer only after I decomposed it into cash start, cash spend, cash earn, and unrealized value.
* I learned that Polars forced me to make explicit many concepts that were hidden implicitly inside loop state.

## Final reflection

This challenge did not just teach me Polars syntax. It forced me to rethink how to model a backtest. In my Numba loop engine, many ideas live implicitly inside mutable state. In Polars, I had to make them explicit through columns, grouped trade summaries, and joins. That made me understand the structure of the backtest more deeply, even if a loop is still the more natural tool for path-dependent execution.
