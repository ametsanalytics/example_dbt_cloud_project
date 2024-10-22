-- imports
{{
    config(
        materialized='table'
    )
}}


with customers as (

    select * from {{ ref('dim_customers') }}

),

orders as (

        select * from {{ ref('stg_orders') }}

),

payments as (

        select * from {{ ref('stg_payments') }}

),

-- cte

-- get paid orders
paid_order as (
        
        select 
            order_id, 
            max(payment_created) as payment_finalized_date, 
            sum(payment_amount) / 100.0 as total_amount_paid
        from payments
            where payment_status <> 'fail'
        group by 1
        
),

-- assign paid orders to customers
customer_paid_order as (

        select 
            Orders.order_id,
            Orders.customer_id,
            Orders.order_placed_at,
            Orders.order_status,
            paid_order.total_amount_paid,
            paid_order.payment_finalized_date
        FROM orders
            left join paid_order ON orders.order_id = paid_order.order_id
            inner join customers on orders.customer_id = customers.customer_id

 ),

-- new and returning customers
-- limetime value

nvsr_ltv as (

    select 
        customers. customer_id,
        first_order_date,
        most_recent_order_date,
        number_of_orders,
        order_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,
-- new and returning customers
    CASE WHEN customers.first_order_date = customer_paid_order.order_placed_at
             THEN 'new'
             ELSE 'return' END as nvsr,
-- limetime value
    sum(total_amount_paid) over (partition by customers.customer_id order by order_placed_at) ltv
    
    from customers
    left join customer_paid_order on customers.customer_id = customer_paid_order.customer_id
),


final as (

    select 
        customer_id,
        first_order_date,
        most_recent_order_date,
        number_of_orders,
        order_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,
        nvsr,
        ltv
    
    from nvsr_ltv
    where order_id is not null
    order by order_id

)

select * from final






