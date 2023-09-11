import snowflake.snowpark.functions as f

def model (dbt,session):
    dbt.config(
        materialized="table",
        database = "DWH"
    )

    stg_orders = dbt.ref("stg_tastybytes_orders")
    stg_order_details = dbt.ref("stg_tastybytes_orderdetails")

    order_summary = stg_order_details.group_by(f.col('order_key')).agg(
        f.sum(f.col("order_item_total_price")).alias("total_order_amount"),
        f.sum(f.col("ORDER_ITEM_DISCOUNT_AMOUNT")).alias("total_discount_amount")
    )

    stg_orders_with_summary = stg_orders.join(
      order_summary,
      stg_orders.order_key == order_summary.order_key,
      join_type="left"
    ).select(
        stg_orders.order_key.alias("order_key"),
        stg_orders.truck_key,
        stg_orders.shift_key,
        stg_orders.order_date,
        order_summary.total_order_amount,
        order_summary.total_discount_amount,
        (order_summary.total_order_amount - order_summary.total_discount_amount).alias("net_amount"),
        ((order_summary.total_discount_amount / order_summary.total_order_amount) * 100).alias("discount_percentage"),
    )

    final_df = stg_orders_with_summary.with_column(
        'Popularity', 
            f.when(f.col("total_order_amount")> 45.000, "High Priority")
                .when(f.col("total_order_amount")>25.000, "Medium Priority")
                    .otherwise("Low Priority")
        
    )

    return final_df 





