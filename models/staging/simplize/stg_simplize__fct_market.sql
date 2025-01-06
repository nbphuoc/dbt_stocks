with
    source as (select * from {{ source("simplize", "fct_market") }}),

    renamed as (
        select
            stock_code as fk_stock_id,
            date as date,
            close_price,
            trading_volume,
            market_cap,
            shares_outstanding,
            close_price * shares_outstanding as calculated_market_cap
        from source
    )

select *
from renamed
