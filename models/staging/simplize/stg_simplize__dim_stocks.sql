with
    source as (select * from {{ source("simplize", "dim_stocks") }}),

    renamed as (
        select
            stock_code as pk_stock_id,
            company_name,
            sector_l1,
            sector_l2,
            sector_l3,
            sector_l4
        from source
    )

select *
from renamed
