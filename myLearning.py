from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder.appName("PySpark_Hadoop")\
.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")\
.master("local[*]")\
.getOrCreate()

# Read CSV File
df = spark.read.csv("hdfs://localhost:9000/demo/employees.csv", header=True, inferSchema=True)

# Show Data
df.show()

# Used to get the schema(data types of column names) of the DataFrame
df.printSchema()

#Convert a column datatype to another datatype
changeDataType = df.select(col("Salary").cast("double"))
changeDataType.printSchema()
changeDataType.show()

# Convert the column "Salary" to double type and create a new column "Tax" with 10% of the salary
# Using expr() to calculate tax
#emp_tax = changeDataType.withColumn("Tax", expr("Salary * 0.1"))
# Using col() to calculate tax
emp_tax = changeDataType.withColumn("Tax", round(col("Salary") * 0.1, 2))
emp_tax.show()

# To Rename a Column
df = df.withColumnRenamed("Senior Management", "Senior_Management")
df.show()

# To insert a new column with a dynamic value based on a condition
gender = df.withColumn("Gender_Short_Form"
              ,when(col("Gender") == 'Male', 'M')
              .when(col("Gender") == 'Female', 'F')
              .otherwise('None'))
gender_1 = df.withColumn("New_Gender", expr("case when Gender = 'Male' then 'M' " \
        "when Gender = 'Female' then 'F' else null end"))


gender_1.show()

# Regular Expression to manipulate the string
# Replace 'A' with 'L' in the "First Name" column (Case Snesitive)
regExp = df.withColumn("New_Name",regexp_replace(col("First Name"), 'A', 'L'))
regExp.show()

# Converting the data type of the "Start Date" column to date
emp_data_date = df.withColumn("Start Date", to_date(col("Start Date"), "M/d/yyyy"))
emp_data_date.show()
emp_data_date.printSchema()
emp_data_date.withColumn("hire_date")