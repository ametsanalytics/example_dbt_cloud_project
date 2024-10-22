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
            paid_order.payment_finalized_date,
            customers.customer_first_name,
            customers.customer_last_name
        FROM orders
            left join paid_order ON orders.order_id = paid_order.order_id
            left join customers on orders.customer_id = customers.customer_id

 ),

-- add more customer order detail to be used in downstream calculations
customer_orders as (
            select 
            customers.customer_id,
            min(order_placed_at) as first_order_date,
            max(order_placed_at) as most_recent_order_date,
            count(order_id) AS number_of_orders
        from customers
        left join Orders
        on orders.customer_id = customers.customer_id 
        group by 1

),

-- new and returning customers

-- ltv

nvsr_ltv as (

    select 
    *,
    CASE WHEN customer_orders.first_order_date = customer_paid_order.order_placed_at
             THEN 'new'
             ELSE 'return' END as nvsr,
    sum(total_amount_paid) over (partition by customer_orders.customer_id order by order_placed_at) ltv
    
    from customer_orders
    left join customer_paid_order on customer_orders.customer_id = customer_paid_order.customer_id
),


final as (

    select * from nvsr_ltv

)

select * from final
order by order_id






-- logic




-- final