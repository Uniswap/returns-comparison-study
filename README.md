# returns-comparison-study
 
## pull_fee_data.py
This pulls daily fee data from the subgraph and places it into sqlite.

## calc_fee_data.py
Calculates the fee returns using the methodology described in the appendix. It places the data into another sqlite database.

## tick_depth.py
Pulls data for the range-bounded pegged assets and stablecoin calculations

## join_all_variables.py
Joins the data (v2/v3 full-range along with range bounded positions) with secondary data like the v3/v2 factories and swap volume from contracts.

