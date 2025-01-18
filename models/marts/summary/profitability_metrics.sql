with
    pnl as (
        select
            -- Dimensions
            p.fk_stock_id,
            p.fk_quarter_id,
            q.quarter,
            q.quarter_start_date,

            -- Measures
            p.net_revenue,
            p.gross_profit,
            p.ebit,
            p.ebitda,
            p.npat,
            p.earnings_adjusted,
            p.cash_earnings,
            p.net_revenue_l4q,
            p.gross_profit_l4q,
            p.ebit_l4q,
            p.ebitda_l4q,
            p.npat_l4q,
            p.earnings_adjusted_l4q,
            p.cash_earnings_l4q,

            -- Last Quarter Growth
            p.net_revenue_growth_yoy,
            p.ebit_growth_yoy,
            p.ebitda_growth_yoy,
            p.npat_growth_yoy,

            -- Last 4 Quarters Growth
            p.net_revenue_l4q_growth_yoy,
            p.ebit_l4q_growth_yoy,
            p.ebitda_l4q_growth_yoy,
            p.npat_l4q_growth_yoy

        from {{ ref("fct_pnl") }} as p
        left join {{ ref("dim_quarters") }} as q on p.fk_quarter_id = q.pk_quarter_id
    ),

    bs as (
        select
            -- Dimensions
            fk_stock_id,
            fk_quarter_id,

            -- Average Measures
            avg_total_assets_2y,
            avg_total_equity_2y,
            avg_total_invested_capital_2y,
            avg_total_fixed_assets_2y

        from {{ ref("fct_bs") }}
    ),

    avg_growth as (
        select
            fk_stock_id,
            fk_quarter_id,
            -- Last 4 quarter
            avg(net_revenue_growth_yoy) over (
                partition by fk_stock_id
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_net_revenue_growth_yoy_4q,
            avg(ebit_growth_yoy) over (
                partition by fk_stock_id
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_ebit_growth_yoy_4q,
            avg(ebitda_growth_yoy) over (
                partition by fk_stock_id
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_ebitda_growth_yoy_4q,
            avg(npat_growth_yoy) over (
                partition by fk_stock_id
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_npat_growth_yoy_4q,

            -- Last 4 years
            avg(net_revenue_l4q_growth_yoy) over (
                partition by fk_stock_id, quarter
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_net_revenue_l4q_growth_yoy_4y,
            avg(ebit_l4q_growth_yoy) over (
                partition by fk_stock_id, quarter
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_ebit_l4q_growth_yoy_4y,
            avg(ebitda_l4q_growth_yoy) over (
                partition by fk_stock_id, quarter
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_ebitda_l4q_growth_yoy_4y,
            avg(npat_l4q_growth_yoy) over (
                partition by fk_stock_id, quarter
                order by quarter_start_date
                rows between 3 preceding and current row
            ) as avg_npat_l4q_growth_yoy_4y

        from pnl
    ),

    margin as (
        select
            fk_stock_id,
            fk_quarter_id,

            -- Last quarter
            try_divide(ebit, net_revenue) as ebit_margin,
            try_divide(gross_profit, net_revenue) as gross_margin,
            try_divide(ebitda, net_revenue) as ebitda_margin,
            try_divide(npat, net_revenue) as npat_margin,

            -- Last 4 quarters
            try_divide(ebit_l4q, net_revenue_l4q) as ebit_margin_l4q,
            try_divide(gross_profit_l4q, net_revenue_l4q) as gross_margin_l4q,
            try_divide(ebitda_l4q, net_revenue_l4q) as ebitda_margin_l4q,
            try_divide(npat_l4q, net_revenue_l4q) as npat_margin_l4q

        from pnl
    ),

    return_on_and_turnover as (
        select
            b.fk_stock_id,
            b.fk_quarter_id,

            -- Return on
            try_divide(p.npat_l4q, b.avg_total_assets_2y) as roa,
            try_divide(p.npat_l4q, b.avg_total_equity_2y) as roe,
            try_divide(p.npat_l4q, b.avg_total_invested_capital_2y) as roic,

            -- Turnover
            try_divide(p.net_revenue, b.avg_total_assets_2y) as avg_asset_turnover,
            try_divide(
                p.net_revenue, b.avg_total_fixed_assets_2y
            ) as avg_fixed_asset_turnover,
            try_divide(p.net_revenue, b.avg_total_equity_2y) as avg_equity_turnover

        from bs as b
        left join
            pnl as p
            on b.fk_stock_id = p.fk_stock_id
            and b.fk_quarter_id = p.fk_quarter_id
    ),

    merged as (
        select
            a.fk_stock_id,
            a.fk_quarter_id,
            a.* except (fk_stock_id, fk_quarter_id),
            m.* except (fk_stock_id, fk_quarter_id),
            p.* except (fk_stock_id, fk_quarter_id),
            r.* except (fk_stock_id, fk_quarter_id)
        from avg_growth as a
        left join
            margin as m
            on a.fk_stock_id = m.fk_stock_id
            and a.fk_quarter_id = m.fk_quarter_id
        left join
            pnl as p
            on a.fk_stock_id = p.fk_stock_id
            and a.fk_quarter_id = p.fk_quarter_id
        left join
            return_on_and_turnover as r
            on a.fk_stock_id = r.fk_stock_id
            and a.fk_quarter_id = r.fk_quarter_id
    ),

    renamed as (
        select
            fk_stock_id,
            fk_quarter_id,

            -- Growth
            avg_net_revenue_growth_yoy_4q as p1__annual_revenues_growth_l4q,
            avg_net_revenue_l4q_growth_yoy_4y as p2__annual_revenues_growth_l4y,
            avg_ebitda_growth_yoy_4q as p3__annual_ebitda_growth_l4q,
            avg_ebitda_l4q_growth_yoy_4y as p4__annual_ebitda_growth_l4y,
            avg_ebit_growth_yoy_4q as p5__annual_ebit_growth_l4q,
            avg_ebit_l4q_growth_yoy_4y as p6__annual_ebit_growth_l4y,
            avg_npat_growth_yoy_4q as p7__annual_npat_growth_l4q,
            avg_npat_l4q_growth_yoy_4y as p8__annual_npat_growth_l4y,

            -- Margin
            gross_margin as p9__gross_margin,
            gross_margin_l4q as p10__gross_margin_l4q,
            ebit_margin as p11__ebit_margin,
            ebit_margin_l4q as p12__ebit_margin_l4q,
            npat_margin as p13__npat_margin,
            npat_margin_l4q as p14__npat_margin_l4q,
            ebitda_margin as p15__ebitda_margin,
            ebitda_margin_l4q as p16__ebitda_margin_l4q,

            -- Adjusted Earnings
            earnings_adjusted as p17__earnings_adjusted,
            earnings_adjusted_l4q as p18__earnings_adjusted_l4q,
            cash_earnings as p19__cash_earnings,
            cash_earnings_l4q as p20__cash_earnings_l4q,

            -- Return on
            roa as p21__roa,
            roe as p22__roe,
            roic as p23__roic,

            -- Turnover
            avg_asset_turnover as p24__avg_asset_turnover,
            avg_fixed_asset_turnover as p25__avg_fixed_asset_turnover,
            avg_equity_turnover as p26__avg_equity_turnover
        from merged
    )

select *
from renamed
