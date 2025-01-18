{%- set surrogate_key_sql = dbt_utils.generate_surrogate_key(
    ["fk_quarter_id", "fk_stock_id"]
) %}
with
    pnl as (select * from {{ ref("stg_simplize__fct_pnl") }}),

    cf as (select fk_stock_id, fk_quarter_id, da, l4q_da from {{ ref("fct_cf") }}),

    joined as (
        select
            pnl.fk_stock_id,
            pnl.fk_quarter_id,
            pnl.* except (fk_stock_id, fk_quarter_id),
            pnl.ebit + cf.da as ebitda,
            pnl.l4q_ebit + cf.l4q_da as l4q_ebitda
        from pnl
        left join
            cf
            on pnl.fk_stock_id = cf.fk_stock_id
            and pnl.fk_quarter_id = cf.fk_quarter_id
    ),

    growth as (
        select
            fk_stock_id,
            fk_quarter_id,

            * except (fk_stock_id, fk_quarter_id),

            -- Last quarter
            case
                when
                    lag(net_revenue) over (
                        partition by fk_stock_id order by fk_quarter_id
                    )
                    < 0
                then 0
                else
                    try_divide(
                        net_revenue,
                        lag(net_revenue) over (
                            partition by fk_stock_id order by fk_quarter_id
                        )
                    )
                    - 1
            end as net_revenue_growth,
            case
                when
                    lag(ebit) over (partition by fk_stock_id order by fk_quarter_id) < 0
                then 0
                else
                    try_divide(
                        ebit,
                        lag(ebit) over (partition by fk_stock_id order by fk_quarter_id)
                    )
                    - 1
            end as ebit_growth,
            case
                when
                    lag(ebitda) over (partition by fk_stock_id order by fk_quarter_id)
                    < 0
                then 0
                else
                    try_divide(
                        ebitda,
                        lag(ebitda) over (
                            partition by fk_stock_id order by fk_quarter_id
                        )
                    )
                    - 1
            end as ebitda_growth,
            case
                when
                    lag(npat) over (partition by fk_stock_id order by fk_quarter_id) < 0
                then 0
                else
                    try_divide(
                        npat,
                        lag(npat) over (partition by fk_stock_id order by fk_quarter_id)
                    )
                    - 1
            end as npat_growth,

            -- Last 4 quarters
            case
                when
                    lag(l4q_net_revenue, 3) over (
                        partition by fk_stock_id order by fk_quarter_id
                    )
                    < 0
                then 0
                else
                    try_divide(
                        l4q_net_revenue,
                        lag(l4q_net_revenue, 3) over (
                            partition by fk_stock_id order by fk_quarter_id
                        )
                    )
                    - 1
            end as l4q_net_revenue_growth,
            case
                when
                    lag(l4q_ebit, 3) over (
                        partition by fk_stock_id order by fk_quarter_id
                    )
                    < 0
                then 0
                else
                    try_divide(
                        l4q_ebit,
                        lag(l4q_ebit, 3) over (
                            partition by fk_stock_id order by fk_quarter_id
                        )
                    )
                    - 1
            end as l4q_ebit_growth,
            case
                when
                    lag(l4q_ebitda, 3) over (
                        partition by fk_stock_id order by fk_quarter_id
                    )
                    < 0
                then 0
                else
                    try_divide(
                        l4q_ebitda,
                        lag(l4q_ebitda, 3) over (
                            partition by fk_stock_id order by fk_quarter_id
                        )
                    )
                    - 1
            end as l4q_ebitda_growth,
            case
                when
                    lag(l4q_npat, 3) over (
                        partition by fk_stock_id order by fk_quarter_id
                    )
                    < 0
                then 0
                else
                    try_divide(
                        l4q_npat,
                        lag(l4q_npat, 3) over (
                            partition by fk_stock_id order by fk_quarter_id
                        )
                    )
                    - 1
            end as l4q_npat_growth
        from joined
    ),

    renamed as (select {{ surrogate_key_sql }} as pk_fct_pnl_id, * from growth)

select *
from renamed
