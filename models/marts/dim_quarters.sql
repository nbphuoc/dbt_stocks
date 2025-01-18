with
    calendar as (
        -- Define the date range
        select
            date_trunc('quarter', date_add(day, n, '2010-01-01')) as quarter_start_date
        from (select explode(sequence(0, datediff('2050-12-31', '2010-01-01'))) as n)
        group by quarter_start_date
    ),

    quarter_details as (
        select
            concat(
                cast(year(quarter_start_date) as string),
                'Q',
                cast(quarter(quarter_start_date) as string)
            ) as pk_quarter_id,
            quarter_start_date::date as quarter_start_date,
            date_add(day, -1, date_add(quarter, 1, quarter_start_date))::date
            as quarter_end_date,
            year(quarter_start_date) as year,
            quarter(quarter_start_date) as quarter,
            concat(
                'Q', quarter(quarter_start_date), ' ', year(quarter_start_date)
            ) as quarter_label,
            datediff(
                date_add(quarter, 1, quarter_start_date), quarter_start_date
            ) as quarter_length_days,
            case
                when
                    quarter_start_date <= current_date
                    and current_date
                    <= date_add(day, -1, date_add(quarter, 1, quarter_start_date))
                then true
                else false
            end as is_current_quarter
        from calendar
    )

select *
from quarter_details
