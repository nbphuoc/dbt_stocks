with
    dim_stocks as (select * from {{ ref("stg_simplize__dim_stocks") }}),
    renamed as (select * from dim_stocks)

select *
from renamed
