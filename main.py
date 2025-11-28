import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

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
    .filter(col("name") == "Children") \
    .groupBy('actor_id', 'first_name','last_name') \
    .agg(count("film_id").alias("total"))

w = Window.orderBy(desc("total"))
# создаем колонку rank
child_actor = actor_counts.withColumn("rank", dense_rank().over(w))
# фильтр на топ3
result = child_actor.filter(col("rank") <=3 )
result.show()

print("Задание №5 - готово!")

#6
df_city = spark.read.jdbc(url = db_url, table = "city", properties = db_properties)
df_address = spark.read.jdbc(url = db_url, table = "address", properties = db_properties)
df_customer = spark.read.jdbc(url = db_url, table = "customer", properties = db_properties)

all_cities = df_city.join(df_address, "city_id") \
    .join(df_customer, "address_id") \
    .groupBy("city") \
    .agg(
        sum(when(col("active") == 1 , 1).otherwise(0)).alias("active_person"),
        sum(when(col("active") == 0, 1).otherwise(0)).alias("inactive_person")) \
    .orderBy(desc("inactive_person"))

all_cities.show()

print("Задание №6 - готово!")



#7
rent_hour_film = df_rental.join(df_inventory, "inventory_id") \
    .join(df_film, "film_id") \
    .join(df_film_category, "film_id") \
    .join(df_category, "category_id") \
    .join(df_customer, "customer_id") \
    .join(df_address, "address_id") \
    .join(df_city, "city_id") \
    .withColumn("hours", (unix_timestamp(col("return_date")) - unix_timestamp(col("rental_date"))) / 3600)



# Фильтруем города на 'a' (или 'A')
rent_hour_film.filter(lower(col("city")).startswith("a")) \
    .groupBy("name") \
    .agg(sum("hours").alias("total_hours")) \
    .orderBy(desc("total_hours")) \
    .limit(1) \
    .show()

# Фильтруем города с дефисом '-'
rent_hour_film.filter(col("city").contains("-")) \
    .groupBy("name") \
    .agg(sum("hours").alias("total_hours")) \
    .orderBy(desc("total_hours")) \
    .limit(1) \
    .show()


print("Задание №7 - готово!")

spark.stop()