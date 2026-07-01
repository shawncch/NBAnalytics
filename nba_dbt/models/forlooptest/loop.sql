
select
    payment_method,
    sum(payment_amount) as total_payment,
    {%- for item in get_payment_methods() %}
    sum(case when payment_method = '{{item}}' then payment_amount end) as {{item}}_payment_method
    {%- if not loop.last %},{% endif -%}
    {% endfor %}
from {{ source('nba_raw', 'sample_transactions') }}
group by 1