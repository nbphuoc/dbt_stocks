{%- set surrogate_key_sql = dbt_utils.generate_surrogate_key(
    ["fk_quarter_id", "fk_stock_id"]
) %}
with
    fct_bs as (select * from {{ ref("stg_simplize__fct_bs") }}),

    renamed as (select {{ surrogate_key_sql }} as pk_fct_bs_id, * from fct_bs)

select *
from renamed
