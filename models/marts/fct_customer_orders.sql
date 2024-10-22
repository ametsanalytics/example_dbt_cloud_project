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

paid_order as (
        
        select 
            order_id, 
            max(payment_created) as payment_finalized_date, 
            sum(payment_amount) / 100.0 as total_amount_paid
        from payments
            where payment_status <> 'fail'
        group by 1
        
),

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

final as (

    select
        customer_paid_order.*,
        ROW_NUMBER() OVER (ORDER BY customer_paid_order.order_id) as transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_orders.customer_id ORDER BY customer_paid_order.order_id) as customer_sales_seq,
        CASE WHEN customer_orders.first_order_date = customer_paid_order.order_placed_at
             THEN 'new'
             ELSE 'return' END as nvsr,
        x.clv_bad as customer_lifetime_value, --- this can be replaced with a window function???
        customer_orders.first_order_date as fdos
    FROM customer_paid_order 
        left join customer_orders on customer_paid_order.customer_id = customer_orders.customer_id
        LEFT OUTER JOIN 
        (
                select
                p.order_id,
                sum(t2.total_amount_paid) as clv_bad
            from customer_paid_order p
            left join customer_paid_order t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
            group by 1
            order by p.order_id
        ) x on x.order_id = p.order_id
        ORDER BY order_id
)

select * from final







-- logic




-- final