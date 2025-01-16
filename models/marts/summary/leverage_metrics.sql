with
    fct_bs as (
        select
            fk_stock_id,
            quarter,
            quarter_start_date,
            tong_tai_san as total_assets,
            total_liabilities,
            von_chu_so_huu as equity,
            long_term_debt,
            short_term_debt,
            long_term_debt + short_term_debt as total_debt,
            lag(long_term_debt + short_term_debt) over (
                partition by fk_stock_id order by quarter_start_date
            ) as prev_total_debt
        from {{ ref("fct_bs") }}
    ),

    fct_pnl as (
        select
            fk_stock_id,
            quarter,
            quarter_start_date,
            sum(loi_nhuan_ke_toan_sau_thue) over (
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
            sum(khau_hao_tai_san_co_dinh + phan_bo_loi_the_thuong_mai) over (
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
            bs.total_debt as l1__total_debt,
            try_divide(
                (pnl.l4q_net_profit + cf.l4q_da), bs.total_liabilities
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
            and bs.quarter_start_date = pnl.quarter_start_date
        left join
            fct_cf as cf
            on bs.fk_stock_id = cf.fk_stock_id
            and bs.quarter_start_date = cf.quarter_start_date
    ),

    renamed as (
        select
            fk_stock_id,
            quarter_start_date,
            quarter,
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
