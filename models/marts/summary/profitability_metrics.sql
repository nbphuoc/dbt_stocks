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
            p.net_revenue_l4q,
            p.gross_profit_l4q,
            p.ebit_l4q,
            p.ebitda_l4q,
            p.npat_l4q,

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

    merged as (
        select
            a.fk_stock_id,
            a.fk_quarter_id,
            a.* except (fk_stock_id, fk_quarter_id),
            m.* except (fk_stock_id, fk_quarter_id)
        from avg_growth as a
        left join
            margin as m
            on a.fk_stock_id = m.fk_stock_id
            and a.fk_quarter_id = m.fk_quarter_id
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
            ebitda_margin_l4q as p16__ebitda_margin_l4q

        from merged
    )

select *
from renamed
