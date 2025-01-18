{%- set surrogate_key_sql = dbt_utils.generate_surrogate_key(
    ["fk_quarter_id", "fk_stock_id"]
) %}
with
    fct_cf as (select * from {{ ref("stg_simplize__fct_cf") }}),

    renamed as (select {{ surrogate_key_sql }} as pk_fct_cf_id, * from fct_cf)

select *
from renamed
