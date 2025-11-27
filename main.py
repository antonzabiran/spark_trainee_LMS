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
# соединение
df_category = spark.read.jdbc(url = db_url,table = "category", properties = db_properties)
df_film_cat = spark.read.jdbc(url = db_url, table = "film_category", properties = db_properties)

count_film_category = df_category.join(df_film_cat, "category_id") \
    .groupBy("name") \
    .agg(count("name").alias("total")) \
    .orderBy(desc("total"))

count_film_category.show()

spark.stop()