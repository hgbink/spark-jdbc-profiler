from pyspark.sql import SparkSession


def get_bounds(spark: SparkSession, table_name, user, password, jdbc_url):
    sql = f'(select min(id) as min, max(id) as max,count(id) as count from {table_name}) as bounds'
    bounds = spark.read.jdbc(
        url=jdbc_url,
        table=sql,
        properties={"user": user, "password": password}
    ).collect()[0]
    return bounds


def get_predicate_sql(key_col, table_name, lower_bound, upper_bound):
    if upper_bound is not None:
        return f"(select * from {table_name} where {key_col} >= {lower_bound} and {key_col} < {upper_bound}) {table_name}"
    else:
        return f"(select * from {table_name} where {key_col} >= {lower_bound}){table_name}"
