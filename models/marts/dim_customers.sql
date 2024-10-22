with customers as (

    select 
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_name 
    from {{ ref('stg_customers') }}

),

orders as (

    select 
        customer_id,
        order_id,
        order_placed_at
    from {{ ref('stg_orders') }}

),

customer_detail as (

        select 
            customers.*,
            min(order_placed_at) as first_order_date,
            max(order_placed_at) as most_recent_order_date,
            count(order_id) AS number_of_orders
        from customers
         left join orders
        on orders.customer_id = customers.customer_id 
        group by 1,2,3,4
)

select * from customer_detail