with
    fct_bs as (
        select
            fk_stock_id,
            fk_quarter_id,
            cash_and_marketable_securities,
            net_cash,
            quick_assets,
            no_ngan_han as current_assets,
            tai_san_ngan_han as current_liabilities,
            shares_outstanding,
            total_debt
        from {{ ref("fct_bs") }}
    ),

    calculated as (
        select
            fk_stock_id,
            fk_quarter_id,
            cash_and_marketable_securities as b1_cash_and_marketable_securities,
            try_divide(
                cash_and_marketable_securities, current_liabilities
            ) as b2_cash_ratio,
            try_divide(current_assets, current_liabilities) as b3_current_ratio,
            try_divide(quick_assets, current_liabilities) as b4_quick_ratio,
            net_cash as b5_net_cash,
            shares_outstanding as b6_shares_outstanding

        from fct_bs
    ),

    renamed as (
        select
            fk_stock_id,
            fk_quarter_id,
            b1_cash_and_marketable_securities,
            b2_cash_ratio,
            b3_current_ratio,
            b4_quick_ratio,
            b5_net_cash,
            b6_shares_outstanding
        from calculated
    )

select *
from renamed
