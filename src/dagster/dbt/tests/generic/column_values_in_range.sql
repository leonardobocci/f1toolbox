--https://docs.getdbt.com/best-practices/writing-custom-generic-tests
{% test column_values_in_range(model, column_name, min_value, max_value) %}

with validation as (
    select
        {{ column_name }} as tested_col
    from {{ model }}
),

validation_errors as (
    select
        tested_col
    from validation
    where tested_col < {{ min_value }} or tested_col > {{ max_value }}
)

select *
from validation_errors

{% endtest %}
