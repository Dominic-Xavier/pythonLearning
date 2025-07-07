
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Initialize Spark Session
spark = SparkSession.builder.appName("PySpark_Hadoop")\
.master("local[*]")\
.getOrCreate()

# Read CSV File
# df = spark.read.csv("hdfs://localhost:9000/demo/employees.csv", header=True, inferSchema=True)
df = spark.read.csv(r"C:\Users\Dominic\Downloads\employees.csv", header=True, inferSchema=True)
df.repartition(8)

# Show Data
df.show()

# Used to get the schema(data types of column names) of the DataFrame
print("Schema")
df.printSchema()

#Convert a column datatype to another datatype
changeDataType = df.select(col("Salary").cast("double"))
changeDataType.printSchema()
print("Converting Salary to double data typea")
changeDataType.show()

# Convert the column "Salary" to double type and create a new column "Tax" with 10% of the salary
# Using expr() to calculate tax
#emp_tax = changeDataType.withColumn("Tax", expr("Salary * 0.1"))
# Using col() to calculate tax
emp_tax = changeDataType.withColumn("Tax", round(col("Salary") * 0.1, 2))
print("Calculating Tax")
emp_tax.show()

# To Rename a Column
df = df.withColumnRenamed("Senior Management", "Senior_Management")
print("Changing column Name")
df.show()

# To insert a new column with a dynamic value based on a condition
print("Doing an operation based on a condition...!")
gender = df.withColumn("Gender_Short_Form"
              ,when(col("Gender") == 'Male', 'M')
              .when(col("Gender") == 'Female', 'F')
              .otherwise('None'))
print("Doing an operation based on a condition...!, but with expr() method")
gender_1 = df.withColumn("New_Gender", expr("case when Gender = 'Male' then 'M' " \
        "when Gender = 'Female' then 'F' else null end"))

gender_1.show()

# Regular Expression to manipulate the string
# Replace 'A' with 'L' in the "First Name" column (Case Snesitive)
print("Renaming a alphabet with regexp_replace() method")
regExp = df.withColumn("New_Name",regexp_replace(col("First Name"), 'A', 'L'))
regExp.show()

# Converting the data type of the "Start Date" column to date
print("Converting Start Date from string to date")
emp_data_date = df.withColumn("Start Date", to_date(col("Start Date"), "M/d/yyyy"))
emp_data_date.show()
emp_data_date.printSchema()

print("Adding the column for current date and time stamp")
currentDate_Time_Stamp = emp_data_date.withColumn("date_now", curdate()).withColumn("Time_Now", current_timestamp())
currentDate_Time_Stamp.show(truncate = False)

# Aggrigating using agg methods
print("Doing aggrigation")
# aggregation = emp_data_date.withColumn("Team", coalesce(col("New_Team"), lit("Other"))).groupBy("Team").agg(sum("Salary").alias("Total_Salary")).where("Total_Salary>8500000")
aggregation = emp_data_date.groupBy("Team").agg(sum("Salary").alias("Total_Salary")).where("Total_Salary>8500000")
aggregation.show()

print("Trying to rename null to other")
# Replace NULLs or empty/whitespace strings in Team column with 'Other'
newEmpData = emp_data_date.withColumn(
    "New_Team",
    when((col("Team").isNull()) | (col("Team") == "") | (col("Team").rlike("^\s*$") | (trim(col("Team")) == "")), "Other")
    .otherwise(col("Team"))
)

newEmpData.show()

#Business Development is getting overridden
print("Trying to do get the highest salary in each team")
win_fun = Window.partitionBy(col("New_Team")).orderBy(col("Salary").desc())
emp2 = newEmpData.withColumn("rn", row_number().over(win_fun))
emp2.show()
# emp3 = newEmpData.withColumn("rn", expr("row_number() over (partition by New_Team order by Salary desc)"))
# emp3.show()

# Using spark.sql method
newEmpData.createOrReplaceTempView("emp_view")

emp4 = spark.sql("""
    SELECT *, row_number() OVER (PARTITION BY New_Team ORDER BY Salary DESC) AS rn
    FROM emp_view
""")
# emp4.show()