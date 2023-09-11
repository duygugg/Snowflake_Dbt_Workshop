def model(dbt,session):
    orders = dbt.ref("stg_tastybytes_orders")
    describe = orders.describe()

    return describe