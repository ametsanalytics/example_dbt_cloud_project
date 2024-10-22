
{% set old_query %}
  select
    order_id,
    total_amount_paid,
    customer_id
  from {{ ref('customer_orders') }}
{% endset %}

{% set new_query %}
  select
    order_id,
    total_amount_paid,
    customer_id
  from {{ ref('fct_customer_orders') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query = old_query,
    b_query = new_query,
    primary_key = "order_id",
    summarize = false
) }}

-- 23
-- 87