with
    fct_bs as (
        select
            fk_stock_id,
            quarter,
            quarter_start_date,
            cash_and_cash_equivalents
            + held_to_maturity_investments
            + held_to_maturity_securities
            + htm_investments as cash_and_marketable_securities,
            current_liabilities,
            current_assets,
            long_term_debt + short_term_debt as total_debt,
            contributed_capital / 10000 as shares_outstanding
        from {{ ref("fct_bs") }}
    ),

    calculated as (
        select
            fk_stock_id,
            quarter_start_date,
            quarter,
            cash_and_marketable_securities,
            cash_and_marketable_securities / current_liabilities as cash_ratio,
            current_assets / current_liabilities as current_ratio,
        from fct_bs
    )

select *
from calculated
