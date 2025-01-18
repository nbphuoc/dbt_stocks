with
    pnl as (
        select
            fk_stock_id,
            fk_quarter_id,
            - cost_of_goods_sold_l4q as cost_of_goods_sold_l4q
        from {{ ref("fct_pnl") }}
    ),

    bs as (
        select
            -- Dimensions
            fk_stock_id,
            fk_quarter_id,

            -- Average L4Q Measures
            avg_total_accounts_receivable_l4q,
            avg_inventory_l4q,
            avg_total_accounts_payable_l4q,

            -- LQ Measures
            net_working_capital_amount,
            net_working_capital_amount
            - net_working_capital_amount_prev_quarter as net_working_capital_change

        from {{ ref("fct_bs") }}
    ),

    days_calculate as (
        select
            -- Dimensions
            p.fk_stock_id,
            p.fk_quarter_id,

            -- Metrics
            try_divide(avg_total_accounts_receivable_l4q, cost_of_goods_sold_l4q)
            * 365 as account_receivable_days,
            try_divide(avg_inventory_l4q, cost_of_goods_sold_l4q)
            * 365 as inventory_days,
            try_divide(avg_total_accounts_payable_l4q, cost_of_goods_sold_l4q)
            * 365 as account_payable_days,
            try_divide(net_working_capital_amount, cost_of_goods_sold_l4q)
            * 365 as net_working_capital_days

        from pnl as p
        left join
            bs as b
            on p.fk_stock_id = b.fk_stock_id
            and p.fk_quarter_id = b.fk_quarter_id
    ),

    merged as (
        select
            b.fk_stock_id,
            b.fk_quarter_id,
            d.* except (fk_stock_id, fk_quarter_id),
            b.* except (fk_stock_id, fk_quarter_id)
        from bs as b
        left join
            days_calculate as d
            on b.fk_stock_id = d.fk_stock_id
            and b.fk_quarter_id = d.fk_quarter_id
    ),

    renamed as (
        select
            -- Dimensions
            fk_stock_id,
            fk_quarter_id,

            -- Metrics
            account_receivable_days as e1__account_receivable_days,
            inventory_days as e2__inventory_days,
            account_payable_days as e3__account_payable_days,
            net_working_capital_days as e4__net_working_capital_days,
            net_working_capital_amount as e5__net_working_capital_amount,
            net_working_capital_change as e6__net_working_capital_change
        from merged
    )

select *
from renamed
