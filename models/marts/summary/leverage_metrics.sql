with
    fct_bs as (
        select
            fk_stock_id,
            quarter,
            quarter_start_date,
            total_assets,
            coalesce(total_liabilities, liabilities) as total_liabilities,
            equity,
            long_term_debt,
            short_term_debt,
            long_term_debt + short_term_debt as total_debt
        from {{ ref("fct_bs") }}
    ),

    fct_pnl as (
        select
            fk_stock_id,
            quarter,
            quarter_start_date,
            sum(net_profit) over (
                partition by fk_stock_id
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as l4q_net_profit
        from {{ ref("fct_pnl") }}
    ),
    fct_cf as (
        select
            fk_stock_id,
            quarter,
            quarter_start_date,
            sum(depreciation + goodwill_amortization) over (
                partition by fk_stock_id
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as l4q_da
        from {{ ref("fct_cf") }}
    ),

    calculated as (
        select
            bs.fk_stock_id,
            bs.quarter_start_date,
            bs.quarter,
            (pnl.l4q_net_profit + cf.l4q_da)
            / bs.total_liabilities as interest_coverage,
            bs.total_liabilities / bs.total_assets as liabilities_to_assets,
            bs.total_liabilities / bs.equity as liabilities_to_equity,
            bs.long_term_debt / bs.equity as long_term_liabilities_to_equity,
            bs.short_term_debt / bs.equity as short_term_liabilities_to_equity,
            bs.total_debt / bs.equity as total_debt_to_equity
        from fct_bs as bs
        left join
            fct_pnl as pnl
            on bs.fk_stock_id = pnl.fk_stock_id
            and bs.quarter_start_date = pnl.quarter_start_date
        left join
            fct_cf as cf
            on bs.fk_stock_id = cf.fk_stock_id
            and bs.quarter_start_date = cf.quarter_start_date
    )

select *
from calculated
