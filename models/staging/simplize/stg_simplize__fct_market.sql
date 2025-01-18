with
    source as (select * from {{ source("simplize", "fct_market") }}),

    renamed as (
        select
            stock_code as fk_stock_id,
            date_format(date, 'yyyyMMdd')::int as fk_date_id,
            concat(year(date)::string, 'Q', quarter(date)::string) as fk_quarter_id,
            close_price,
            trading_volume,
            market_cap,
            shares_outstanding,
            close_price * shares_outstanding as calculated_market_cap
        from source
    )

select *
from renamed
