with
    fct_cf as (select * from {{ ref("fct_cf") }}),

    fct_bs as (select * from {{ ref("fct_bs") }}),

    fct_pnl as (select * from {{ ref("fct_pnl") }}),

    valuations as (
        select fk_stock_id, fk_date_id, fk_quarter_id, v1__market_cap as market_cap
        from {{ ref("valuation_metrics") }}
    ),

    efficiency as (
        select
            *,
            e6__net_working_capital_change as net_working_capital_change,
            sum(e6__net_working_capital_change) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as net_working_capital_change_l4q
        from {{ ref("efficiency_metrics") }}
    ),

    leverage as (
        select
            fk_stock_id,
            fk_quarter_id,
            l8__debt_change as debt_change,
            sum(l8__debt_change) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as debt_change_l4q
        from {{ ref("leverage_metrics") }}
    ),

    joined as (
        select
            c.fk_stock_id,
            c.fk_quarter_id,
            v.fk_date_id,

            try_divide(- c.dividend_paid_l4q, p.npat_l4q) as dividend_payout_ratio_l4q,
            try_divide(
                - c.dividend_paid_l16q, p.npat_l16q
            ) as dividend_payout_ratio_l4y,
            try_divide(- c.dividend_paid_l4q, v.market_cap) as dividend_yield_l4q,
            try_divide(
                - c.avg_dividend_paid_l4q_4y, v.market_cap
            ) as dividend_yield_l4y,
            p.cash_earnings
            - e.net_working_capital_change
            + c.capex as free_cash_flow_to_firm,
            p.cash_earnings_l4q
            - e.net_working_capital_change_l4q
            + c.capex_l4q as free_cash_flow_to_firm_l4q,
            free_cash_flow_to_firm + l.debt_change as free_cash_flow_to_equity,
            free_cash_flow_to_firm_l4q
            + l.debt_change_l4q as free_cash_flow_to_equity_l4q

        from valuations as v
        left join
            fct_cf as c
            on v.fk_stock_id = c.fk_stock_id
            and v.fk_quarter_id = c.fk_quarter_id
        left join
            fct_pnl as p
            on v.fk_stock_id = p.fk_stock_id
            and v.fk_quarter_id = p.fk_quarter_id
        left join
            fct_bs as b
            on v.fk_stock_id = b.fk_stock_id
            and v.fk_quarter_id = b.fk_quarter_id
        left join
            efficiency as e
            on v.fk_stock_id = e.fk_stock_id
            and v.fk_quarter_id = e.fk_quarter_id
        left join
            leverage as l
            on v.fk_stock_id = l.fk_stock_id
            and v.fk_quarter_id = l.fk_quarter_id

    ),

    renamed as (
        select
            -- Dimensions
            fk_stock_id,
            fk_quarter_id,
            fk_date_id,

            -- Metrics
            dividend_payout_ratio_l4q as c1__dividend_payout_ratio_l4q,
            dividend_payout_ratio_l4y as c2__dividend_payout_ratio_l4y,
            dividend_yield_l4q as c3__dividend_yield_l4q,
            dividend_yield_l4y as c4__dividend_yield_l4y,
            free_cash_flow_to_firm as c5__free_cash_flow_to_firm,
            free_cash_flow_to_firm_l4q as c6__free_cash_flow_to_firm_l4q,
            free_cash_flow_to_equity as c7__free_cash_flow_to_equity,
            free_cash_flow_to_equity_l4q as c8__free_cash_flow_to_equity_l4q
        from joined
    )

select *
from renamed
