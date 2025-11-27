import os
from pyspark.sql import SparkSession

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
df = spark.read.jdbc(
    url=db_url,
    table = "category",
    properties=db_properties
)

df.show()

spark.stop()