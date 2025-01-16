with
    fct_bs as (
        select
            fk_stock_id,
            quarter,
            quarter_start_date,
            cash_and_marketable_securities,
            net_cash,
            quick_assets,
            no_ngan_han as current_assets,
            tai_san_ngan_han as current_liabilities,
            total_debt,
            von_gop_cua_chu_so_huu / 10000 as shares_outstanding
        from {{ ref("fct_bs") }}
    ),

    calculated as (
        select
            fk_stock_id,
            quarter_start_date,
            quarter,
            cash_and_marketable_securities as b1_cash_and_marketable_securities,
            try_divide(
                cash_and_marketable_securities, current_liabilities
            ) as b2_cash_ratio,
            try_divide(current_assets, current_liabilities) as b3_current_ratio,
            try_divide(quick_assets, current_liabilities) as b4_quick_ratio,
            net_cash as b5_net_cash,
            shares_outstanding as b6_shares_outstanding

        from fct_bs
    )

select *
from calculated
