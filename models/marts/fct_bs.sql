{%- set surrogate_key_sql = dbt_utils.generate_surrogate_key(
    ["fk_quarter_id", "fk_stock_id"]
) %}
with
    fct_bs as (select * from {{ ref("stg_simplize__fct_bs") }}),

    quarters as (select * from {{ ref("dim_quarters") }}),

    average as (
        select
            b.fk_stock_id,
            b.fk_quarter_id,
            b.* except (fk_stock_id, fk_quarter_id),

            -- Previous year
            avg(b.total_assets) over (
                partition by b.fk_stock_id, q.quarter
                order by b.fk_quarter_id
                rows between 1 preceding and current row
            ) as avg_total_assets_2y,
            avg(b.total_equity) over (
                partition by b.fk_stock_id, q.quarter
                order by b.fk_quarter_id
                rows between 1 preceding and current row
            ) as avg_total_equity_2y,
            avg(b.total_invested_capital) over (
                partition by b.fk_stock_id, q.quarter
                order by b.fk_quarter_id
                rows between 1 preceding and current row
            ) as avg_total_invested_capital_2y,
            avg(b.total_fixed_assets) over (
                partition by b.fk_stock_id, q.quarter
                order by b.fk_quarter_id
                rows between 1 preceding and current row
            ) as avg_total_fixed_assets_2y
        from fct_bs as b
        left join quarters as q on b.fk_quarter_id = q.pk_quarter_id
    ),

    prev_quarter as (
        select
            *,
            lag(net_working_capital_amount) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as net_working_capital_amount_prev_quarter
        from average
    ),

    renamed as (select {{ surrogate_key_sql }} as pk_fct_bs_id, * from prev_quarter)

select *
from renamed
