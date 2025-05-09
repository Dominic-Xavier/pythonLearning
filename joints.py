from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Initialize Spark Session
spark = SparkSession.builder.appName("PySpark_Hadoop")\
.master("local[*]")\
.getOrCreate()

customers = spark.read.csv("csvFiles/customers-100.csv", header=True, inferSchema=True).alias("c")
customers.repartition(8)

people = spark.read.csv("csvFiles\people-100.csv", header=True, inferSchema=True).alias("p")
people.repartition(8)

products = spark.read.csv("csvFiles\products-100.csv", header=True, inferSchema=True).alias("pr")
products.repartition(8)

organizations = spark.read.csv("csvFiles\organizations-100.csv", header=True, inferSchema=True).alias("o")
organizations.repartition(8)

leads = spark.read.csv("csvFiles\leads-100.csv", header=True, inferSchema=True).alias("l")
leads.repartition(8)

df = customers.join(people, customers["Index"] == people["Index"], "Inner")
df2 = df.join(products, people["Index"] == products["Index"], "Inner")
df3 = df2.join(organizations, products["Index"] == organizations["Index"], "Inner")
final_df = df3.join(leads, organizations["Index"] == leads["Index"], "Inner")
# final_df.select("c.Customer Id", "c.First Name", "c.Last Name", "pr.Name as Product_Name", "pr.Description", "pr.Brand", "pr.Price", "pr.Availability", "o.Name", "l.Lead Owner").show(truncate = False)
final_df.select(
    col("c.Customer Id"),
    col("c.First Name"),
    col("c.Last Name"),
    col("pr.Name").alias("Product_Name"),
    col("pr.Description").alias("Product_Desc"),
    col("pr.Brand"),
    col("pr.Price"),
    col("pr.Availability"),
    col("o.Name"),
    col("l.Lead Owner").alias("Lead Owner")
).show(truncate=False)

 