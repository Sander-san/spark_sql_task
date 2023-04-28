import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


'''Declare variables'''
load_dotenv()
DB_NAME = os.environ.get('DB_NAME')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_USER = os.environ.get('DB_USER')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
JDBC_DRIVER_PATH = os.environ.get('JDBC_DRIVER_PATH')


'''Create session'''
spark = SparkSession.builder \
    .master('local[1]') \
    .appName('task') \
    .config('spark.driver.extraClassPath', JDBC_DRIVER_PATH) \
    .getOrCreate()


def create_dataframe(dbtable):
    df = spark.read.format('jdbc') \
        .option('url', f'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}') \
        .option('dbtable', dbtable) \
        .option('user', DB_USER) \
        .option('password', DB_PASSWORD) \
        .load()
    return df


'''Extract data from db to DataFrame'''
city_df = create_dataframe('city')
address_df = create_dataframe('address')
category_df = create_dataframe('category')
film_category_df = create_dataframe('film_category')
film_df = create_dataframe('film')
film_actor_df = create_dataframe('film_actor')
inventory_df = create_dataframe('inventory')
rental_df = create_dataframe('rental')
actor_df = create_dataframe('actor')
customer_df = create_dataframe('customer')
payment_df = create_dataframe('payment')


'''Task 1'''
category_df = category_df.select(*category_df.columns[:-1])
film_category_df = film_category_df.select(*film_category_df.columns[:-1])

df1 = category_df.join(film_category_df, 'category_id')
df1 = df1.groupBy('name') \
    .agg(count('film_id').alias('film_quantity')) \
    .orderBy(col('film_quantity').desc())
df1.show()


'''Task 2'''
df2 = (actor_df.select('actor_id', concat_ws(' ',
                                             actor_df.first_name,
                                             actor_df.last_name).alias('full_name'))
       .join(film_actor_df, 'actor_id')
       .join(film_df, 'film_id')
       )
df2 = df2.select('full_name', 'rental_duration')\
    .groupBy('full_name') \
    .agg(sum('rental_duration').alias('general_rent_time')) \
    .orderBy(col('general_rent_time').desc())
# df2.select('full_name').show(10)


'''Task 3'''
df3 = (category_df
       .join(film_category_df, 'category_id')
       .join(film_df, 'film_id')
       .join(inventory_df, 'film_id')
       .join(rental_df, 'inventory_id')
       .join(payment_df, 'rental_id')
       )
df3 = df3.groupBy('name') \
    .agg(sum('amount').alias('total_amount')) \
    .orderBy(col('total_amount').desc())
# df3.select('name').show(1)


'''Task 4'''
df4 = film_df.select('title', 'film_id')
df4 = df4.join(inventory_df, on=['film_id'], how='left')
df4 = df4.filter(col('inventory_id').isNull())
df4 = df4.select('title').distinct()
# df4.show()


'''Task 5'''
df5 = actor_df.select('actor_id',
                      concat_ws(' ', actor_df.first_name, actor_df.last_name)
                      .alias('full_name'))
df5 = df5.join(film_actor_df, on=['actor_id'], how='inner')
df5 = df5.join(film_df, on=['film_id'], how='inner')
df5 = df5.select('film_id', 'actor_id', 'full_name')
df5 = df5.join(film_category_df, on=['film_id'], how='inner')
df5 = df5.join(category_df, on=['category_id'], how='inner')
df5 = df5.filter("name == 'Children'")
df5 = df5.groupBy('full_name') \
    .agg(count('actor_id').alias('count')) \
    .orderBy(col('count').desc())
w = Window.orderBy(col('count').desc())
df5 = df5.withColumn('rank', dense_rank().over(w))
df5 = df5.select('full_name').filter("rank in (1, 2, 3)")
# df5.show()

'''Task 6'''
df6 = customer_df.join(address_df, "address_id").join(city_df, "city_id")
df6 = df6.groupBy('city').agg(
    count(when(df6.active == 1, 1)).alias('active_customers'),
    count(when(df6.active == 0, 1)).alias('inactive_customers')
).orderBy('active_customers')
# df6.show(50)


'''Task 7'''
df7 = (
    customer_df.select('customer_id', 'address_id')
    .join(address_df.select('city_id', 'address_id'), "address_id")
    .join(city_df.select('city_id', 'city'), "city_id")
    .join(rental_df.select('customer_id', 'inventory_id', 'rental_date', 'return_date'), "customer_id")
    .join(inventory_df.select('inventory_id', 'film_id'), "inventory_id")
    .join(film_df.select('film_id', 'title'), "film_id")
    .join(film_category_df.select('film_id', 'category_id'), "film_id")
    .join(category_df.select('category_id', 'name'), "category_id")
)
df7 = df7.withColumn('rental_days', datediff('return_date', 'rental_date'))

res_a = df7.filter("city like '%-%'").groupBy('name')\
    .agg(sum('rental_days').alias('total'))\
    .orderBy('total', ascending=False)

res_b = df7.filter("title like 'A%'").groupBy('name')\
    .agg(sum('rental_days').alias('total'))\
    .orderBy('total', ascending=False)

res = res_b.union(res_b)
# res.select('name').show(1)

