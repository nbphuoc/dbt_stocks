with
    fct_bs as (
        select
            fk_stock_id,
            fk_quarter_id,
            tong_tai_san as total_assets,
            total_liabilities,
            von_chu_so_huu as equity,
            long_term_debt,
            short_term_debt,
            long_term_debt + short_term_debt as total_debt,
            lag(long_term_debt + short_term_debt) over (
                partition by fk_stock_id order by fk_quarter_id
            ) as prev_total_debt
        from {{ ref("fct_bs") }}
    ),

    fct_pnl as (select fk_stock_id, fk_quarter_id, npat_l4q from {{ ref("fct_pnl") }}),
    fct_cf as (select fk_stock_id, fk_quarter_id, da_l4q from {{ ref("fct_cf") }}),

    calculated as (
        select
            bs.fk_stock_id,
            bs.fk_quarter_id,
            bs.total_debt as l1__total_debt,
            pnl.npat_l4q,
            cf.da_l4q,
            bs.total_liabilities,
            try_divide(
                (pnl.npat_l4q + cf.da_l4q), bs.total_liabilities
            ) as l2__interest_coverage,
            try_divide(
                bs.total_liabilities, bs.total_assets
            ) as l3__liabilities_to_assets,
            try_divide(bs.total_liabilities, bs.equity) as l4__liabilities_to_equity,
            try_divide(bs.long_term_debt, bs.equity) as l5__lt_debt_to_equity,
            try_divide(bs.short_term_debt, bs.equity) as l6__st_debt_to_equity,
            try_divide(bs.total_debt, bs.equity) as l7__total_debt_to_equity,
            bs.total_debt - bs.prev_total_debt as l8__debt_change
        from fct_bs as bs
        left join
            fct_pnl as pnl
            on bs.fk_stock_id = pnl.fk_stock_id
            and bs.fk_quarter_id = pnl.fk_quarter_id
        left join
            fct_cf as cf
            on bs.fk_stock_id = cf.fk_stock_id
            and bs.fk_quarter_id = cf.fk_quarter_id
    ),

    renamed as (
        select
            fk_stock_id,
            fk_quarter_id,
            l1__total_debt,
            l2__interest_coverage,
            l3__liabilities_to_assets,
            l4__liabilities_to_equity,
            l5__lt_debt_to_equity,
            l6__st_debt_to_equity,
            l7__total_debt_to_equity,
            l8__debt_change
        from calculated
    )

select *
from renamed
order by fk_stock_id, fk_quarter_id
