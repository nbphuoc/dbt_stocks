{%- set surrogate_key_sql = dbt_utils.generate_surrogate_key(
    ["fk_quarter_id", "fk_stock_id"]
) %}
with
    pnl as (select * from {{ ref("stg_simplize__fct_pnl") }}),

    cf as (select fk_stock_id, fk_quarter_id, da, da_l4q from {{ ref("fct_cf") }}),

    joined as (
        select
            pnl.fk_stock_id,
            pnl.fk_quarter_id,
            pnl.* except (fk_stock_id, fk_quarter_id),
            pnl.ebit + cf.da as ebitda,
            pnl.ebit_l4q + cf.da_l4q as ebitda_l4q,
            pnl.earnings_adjusted + cf.da as cash_earnings,
            pnl.earnings_adjusted_l4q + cf.da_l4q as cash_earnings_l4q
        from pnl
        left join
            cf
            on pnl.fk_stock_id = cf.fk_stock_id
            and pnl.fk_quarter_id = cf.fk_quarter_id
    ),

    prev_year as (
        select
            *,

            -- Last quarter
            lag(net_revenue, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as net_revenue_prev_year,
            lag(ebit, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as ebit_prev_year,
            lag(ebitda, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as ebitda_prev_year,
            lag(npat, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as npat_prev_year,

            -- Last 4 quarters
            lag(net_revenue_l4q, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as net_revenue_l4q_prev_year,
            lag(ebit_l4q, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as ebit_l4q_prev_year,
            lag(ebitda_l4q, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as ebitda_l4q_prev_year,
            lag(npat_l4q, 4) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as npat_l4q_prev_year
        from joined
    ),

    growth as (
        select
            *,
            case
                when net_revenue_prev_year < 0
                then 0
                else try_divide(net_revenue, net_revenue_prev_year) - 1
            end as net_revenue_growth_yoy,
            case
                when ebit_prev_year < 0 then 0 else try_divide(ebit, ebit_prev_year) - 1
            end as ebit_growth_yoy,
            case
                when ebitda_prev_year < 0
                then 0
                else try_divide(ebitda, ebitda_prev_year) - 1
            end as ebitda_growth_yoy,
            case
                when npat_prev_year < 0 then 0 else try_divide(npat, npat_prev_year) - 1
            end as npat_growth_yoy,

            case
                when net_revenue_l4q_prev_year < 0
                then 0
                else try_divide(net_revenue_l4q, net_revenue_l4q_prev_year) - 1
            end as net_revenue_l4q_growth_yoy,
            case
                when ebit_l4q_prev_year < 0
                then 0
                else try_divide(ebit_l4q, ebit_l4q_prev_year) - 1
            end as ebit_l4q_growth_yoy,
            case
                when ebitda_l4q_prev_year < 0
                then 0
                else try_divide(ebitda_l4q, ebitda_l4q_prev_year) - 1
            end as ebitda_l4q_growth_yoy,
            case
                when npat_l4q_prev_year < 0
                then 0
                else try_divide(npat_l4q, npat_l4q_prev_year) - 1
            end as npat_l4q_growth_yoy

        from prev_year
    ),

    renamed as (select {{ surrogate_key_sql }} as pk_fct_pnl_id, * from growth)

select *
from renamed
