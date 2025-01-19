with
    fct_bs as (
        select
            fk_stock_id,
            fk_quarter_id,
            shares_outstanding,
            net_cash,
            total_equity,
            avg_total_equity_l4q,
            net_cash
        from {{ ref("fct_bs") }}
    ),

    fct_pnl as (
        select
            fk_stock_id,
            fk_quarter_id,
            npat_l4q,
            avg_npat_l4q_4y,
            net_revenue_l4q,
            avg_net_revenue_l4q_4y,
            ebitda_l4q,
            avg_ebitda_l4q_4y,
            earnings_adjusted_l4q,
            avg_earnings_adjusted_l4q_4y,
            cash_earnings_l4q,
            avg_cash_earnings_l4q_4y
        from {{ ref("fct_pnl") }}
    ),

    profitability as (
        select fk_stock_id, fk_quarter_id, p6__annual_ebit_growth_l4y
        from {{ ref("profitability_metrics") }}
    ),

    markets as (
        select fk_stock_id, fk_date_id, fk_quarter_id, close_price
        from {{ ref("fct_markets") }}
    ),

    joined as (
        select
            m.fk_stock_id,
            m.fk_date_id,
            m.fk_quarter_id,

            -- Basic metrics
            b.shares_outstanding * m.close_price as market_cap,
            market_cap + b.net_cash as enterprise_value,
            try_divide(b.total_equity, b.shares_outstanding) as book_value_per_share,
            try_divide(b.net_cash, market_cap) as net_cash_to_market_cap,

            -- Last 4 quarters
            try_divide(p.npat_l4q, b.shares_outstanding) as earnings_per_share_l4q,
            try_divide(market_cap, p.npat_l4q) as price_to_earnings_l4q,
            try_divide(market_cap, b.avg_total_equity_l4q) as price_to_book_l4q,
            try_divide(market_cap, p.net_revenue_l4q) as price_to_sales_l4q,
            try_divide(enterprise_value, p.ebitda_l4q) as ev_to_ebitda_l4q,
            try_divide(
                market_cap, p.earnings_adjusted_l4q
            ) as price_to_earnings_adjusted_l4q,
            try_divide(
                enterprise_value, p.cash_earnings_l4q
            ) as ev_to_cash_earnings_l4q,

            -- Last 4 years
            try_divide(
                p.avg_npat_l4q_4y, b.shares_outstanding
            ) as earnings_per_share_l4y,
            try_divide(market_cap, p.avg_npat_l4q_4y) as price_to_earnings_l4y,
            try_divide(market_cap, p.avg_net_revenue_l4q_4y) as price_to_sales_l4y,
            try_divide(
                price_to_earnings_l4y, p2.p6__annual_ebit_growth_l4y
            ) as pe_to_growth_l4y,
            try_divide(enterprise_value, p.avg_ebitda_l4q_4y) as ev_to_ebitda_l4y,
            try_divide(
                market_cap, p.avg_earnings_adjusted_l4q_4y
            ) as price_to_earnings_adjusted_l4y,
            try_divide(
                enterprise_value, p.avg_cash_earnings_l4q_4y
            ) as ev_to_cash_earnings_l4y

        from markets as m
        left join
            fct_bs as b
            on m.fk_stock_id = b.fk_stock_id
            and m.fk_quarter_id = b.fk_quarter_id
        left join
            fct_pnl as p
            on m.fk_stock_id = p.fk_stock_id
            and m.fk_quarter_id = p.fk_quarter_id
        left join
            profitability as p2
            on m.fk_stock_id = p2.fk_stock_id
            and m.fk_quarter_id = p2.fk_quarter_id
        where
            b.fk_stock_id is not null
            and p.fk_stock_id is not null
            and p2.fk_stock_id is not null

    ),

    renamed as (
        select
            fk_stock_id,
            fk_quarter_id,
            fk_date_id,
            market_cap as v1__market_cap,
            enterprise_value as v2__enterprise_value,
            book_value_per_share as v3__book_value_per_share,
            earnings_per_share_l4q as v4__earnings_per_share_l4q,
            earnings_per_share_l4y as v5__earnings_per_share_l4y,
            price_to_earnings_l4q as v6__price_to_earnings_l4q,
            price_to_earnings_l4y as v7__price_to_earnings_l4y,
            price_to_book_l4q as v8__price_to_book_l4q,
            price_to_sales_l4q as v9__price_to_sales_l4q,
            price_to_sales_l4y as v10__price_to_sales_l4y,
            pe_to_growth_l4y as v11__pe_to_growth_l4y,
            ev_to_ebitda_l4q as v12__ev_to_ebitda_l4q,
            ev_to_ebitda_l4y as v13__ev_to_ebitda_l4y,
            price_to_earnings_adjusted_l4q as v14__price_to_earnings_adjusted_l4q,
            price_to_earnings_adjusted_l4y as v15__price_to_earnings_adjusted_l4y,
            ev_to_cash_earnings_l4q as v16__ev_to_cash_earnings_l4q,
            ev_to_cash_earnings_l4y as v17__ev_to_cash_earnings_l4y
        from joined
    )

select *
from renamed
