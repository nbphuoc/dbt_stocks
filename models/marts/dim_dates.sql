with
    raw_generated_data as (
        {{ dbt_date.get_date_dimension("2010-01-01", "2050-12-31") }}
    ),

    renamed as (
        select replace(date_day, '-', '') as pk_date_id, * from raw_generated_data
    )

select *
from renamed
