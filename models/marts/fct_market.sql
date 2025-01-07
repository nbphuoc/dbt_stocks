{%- set surrogate_key_sql = dbt_utils.generate_surrogate_key(["date", "fk_stock_id"]) %}
with
    fct_pnl as (select * from {{ ref("stg_simplize__fct_market") }}),

    renamed as (select {{ surrogate_key_sql }} as pk_fct_market_id, * from fct_pnl)

select *
from renamed
