import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum

# путь к драйверу
driver_path = os.path.abspath("postgresql-42.7.2.jar")


spark = SparkSession.builder \
    .appName("Pagila manual ") \
    .config("spark.driver.extraClassPath", driver_path) \
    .config("spark.jars", driver_path) \
    .getOrCreate()

# адрес БД
db_url = "jdbc:postgresql://localhost:5555/pagila"
# подключение к БД
db_properties = {
    "user": "postgres",
    "password": "12345",
    "database": "pagila",
    "driver": "org.postgresql.Driver"
}
#1
df_category = spark.read.jdbc(url = db_url,table = "category", properties = db_properties)
df_film_cat = spark.read.jdbc(url = db_url, table = "film_category", properties = db_properties)

count_film_category = df_category.join(df_film_cat, "category_id") \
    .groupBy("name") \
    .agg(count("name").alias("total")) \
    .orderBy(desc("total"))

count_film_category.show()
print("Задание №1 - готово!")


#2
df_actor = spark.read.jdbc(url = db_url, table = "actor", properties = db_properties)
df_film_actor = spark.read.jdbc(url = db_url, table = "film_actor", properties = db_properties)
df_inventory = spark.read.jdbc(url = db_url, table = "inventory", properties = db_properties)
df_rental = spark.read.jdbc(url = db_url, table = "rental", properties = db_properties)

count_actor_names = df_actor.join(df_film_actor, "actor_id") \
    .join(df_inventory, "film_id") \
    .join(df_rental, "inventory_id") \
    .groupBy("actor_id", "first_name", "last_name") \
    .agg(count("inventory_id").alias ("count_rent_actor")) \
    .orderBy(desc("count_rent_actor")) \
    .limit(10)

count_actor_names.show()
print("Задание №2 - готово!")

#3
df_payment = spark.read.jdbc(url = db_url, table = "payment", properties = db_properties)
df_rental = spark.read.jdbc(url = db_url, table = "rental", properties = db_properties)
df_inventory = spark.read.jdbc(url = db_url, table = "inventory", properties = db_properties)
df_film_category = spark.read.jdbc(url = db_url, table = "film_category", properties = db_properties)
df_category = spark.read.jdbc(url = db_url, table = "category", properties = db_properties)

sum_payment_amount = df_payment.join(df_rental,"rental_id") \
    .join(df_inventory, "inventory_id") \
    .join(df_film_category, "film_id") \
    .join(df_category, "category_id") \
    .groupBy("name") \
    .agg(sum("amount").alias("total_price")) \
    .orderBy(desc("total_price")) \
    .limit(1)

sum_payment_amount.show()
print("Задание №3 - готово!")


#4
df_film = spark.read.jdbc(url = db_url, table = "film", properties = db_properties)
df_inventory = spark.read.jdbc(url = db_url, table = "inventory", properties = db_properties)

name_fill_no_in_inventary = df_film.join(df_inventory,"film_id","left_anti" )

name_fill_no_in_inventary.show()

print("Задание №4 - готово!")


#5

df_actor = spark.read.jdbc(url = db_url, table = "actor", properties = db_properties)
df_film_actor = spark.read.jdbc(url = db_url, table = "film_actor", properties = db_properties)
df_film = spark.read.jdbc(url = db_url, table = "film", properties = db_properties)
df_film_category = spark.read.jdbc(url = db_url, table = "film_category", properties = db_properties)
df_category = spark.read.jdbc( url = db_url, table = "category", properties = db_properties)

actor_counts = df_actor.join(df_film_actor, "actor_id") \
    .join(df_film_category, "film_id") \
    .join(df_category, "category_id") \
    .filter(col("name" == "Children")) \
    .groupBy('actor_id', 'first_name','last_name') \
    .agg(count("film_id").alias("total") \




spark.stop()