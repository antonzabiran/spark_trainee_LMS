import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

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

spark.stop()