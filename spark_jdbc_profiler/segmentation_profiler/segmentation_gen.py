from pyspark.sql import SparkSession


def get_bounds(spark: SparkSession, jdbc_url, connnection_properties, table_name, num_of_partitions):
    sql = f'(select min(id) as min, max(id) as max,count(id) as count from {table_name}) as bounds'

    bounds = spark.read.jdbc(
        url=jdbc_url,
        table=sql,
        properties=connnection_properties
    ).collect()[0]

    length = ((bounds.max - bounds.min)//num_of_partitions)

    return [{"min": bounds.min + length*i, "max": bounds.min + length*(i+1)} for i in range(num_of_partitions)]


def get_predicate_sql(key_col, table_name, lower_bound, upper_bound):
    if upper_bound is not None:
        return f"(select * from {table_name} where {key_col} >= {lower_bound} and {key_col} < {upper_bound}) {table_name}"
    else:
        return f"(select * from {table_name} where {key_col} >= {lower_bound}){table_name}"
