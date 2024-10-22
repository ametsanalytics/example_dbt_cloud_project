-- imports

with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

        select * from {{ ref('stg_orders') }}

),

payments as (

        select * from {{ ref('stg_payments') }}

),

paid_order as (
        
        select ORDERID as order_id, 
            max(CREATED) as payment_finalized_date, 
            sum(AMOUNT) / 100.0 as total_amount_paid
        from raw.stripe.payment
            where STATUS <> 'fail'
        group by 1
)




-- cte




-- logic




-- final