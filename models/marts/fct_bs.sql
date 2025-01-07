{%- set surrogate_key_sql = dbt_utils.generate_surrogate_key(
    ["quarter", "fk_stock_id"]
) %}
with
    fct_bs as (select * from {{ ref("stg_simplize__fct_bs") }}),

    renamed as (select {{ surrogate_key_sql }} as pk_fct_bs_d, * from fct_bs)

select *
from renamed
