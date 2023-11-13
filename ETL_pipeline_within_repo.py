# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Create Spark session
spark = SparkSession.builder.appName("School Attendance ETL").getOrCreate()

# Set up the Blob storage account access key
spark.conf.set(
    "fs.azure.account.key.<individual3>.blob.core.windows.net",
    "<RvtG/R+z25TAAm6LdgMMfxV9zJT/4A20TYb2O30T+EeliwJFKoONpPZvQt25WHFP9fTVBcbmInd5+AStlw2TzA==>",
)


# COMMAND ----------

# Read the CSV file from Blob storage
storage_account_name = "individual3"
storage_account_access_key = "RvtG/R+z25TAAm6LdgMMfxV9zJT/4A20TYb2O30T+EeliwJFKoONpPZvQt25WHFP9fTVBcbmInd5+AStlw2TzA=="

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_access_key,
)

csv_file_path = "wasbs://schoolattendance@individual3.blob.core.windows.net/School_Attendance_by_Student_Group_and_District__2021-2022.csv"
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
df.show()


# COMMAND ----------

# Transform Data Using SQL
# Register the DataFrame as a temp view
df.createOrReplaceTempView("School_Attendance_Table")

# Example Transformation: Create new table with specific columns
transformed_df = spark.sql(
    """
    SELECT 
        "District code" AS District_code, 
        "District name" AS District_name, 
        Category, 
        "Student group" AS Student_group, 
        "2021-2022_student_count_-_year_to_date" AS Student_count, 
        "2021-2022_attendance_rate_-_year_to_date" AS Attendance_rate 
    FROM School_Attendance_Table
"""
)
transformed_df.show()

# COMMAND ----------

# Load Transformed Data into Delta Lake
# Define the path to store the Delta table
delta_table_path = "/mnt/delta/School_Attendance_Transformed"

# Write the DataFrame as a Delta table
transformed_df.write.format("delta").mode("overwrite").save(delta_table_path)

# COMMAND ----------

# Read from Delta Lake
delta_df = spark.read.format("delta").load(delta_table_path)
delta_df.show()

# COMMAND ----------

# Interact with the Delta Table using Spark SQL:
# Read the Delta table
delta_df = spark.read.format("delta").load(delta_table_path)

# Register the DataFrame as a temporary view
delta_df.createOrReplaceTempView("school_attendance_delta")

# Use Spark SQL to interact with the view
spark.sql("SELECT * FROM school_attendance_delta LIMIT 10").show()


# COMMAND ----------


from pyspark.sql.functions import col

# Example validation: Check for null values in key columns
if (
    delta_df.filter(
        col("District_code").isNull() | col("Attendance_rate").isNull()
    ).count()
    > 0
):
    raise Exception("Data validation failed: Null values found in key columns")


# COMMAND ----------

# Data Validation and Quality Checks
# Check for null values in key columns
transformed_df.select(
    [
        count(when(isnan(c) | col(c).isNull(), c)).alias(c)
        for c in transformed_df.columns
    ]
).show()


# COMMAND ----------

# Visualization and Conclusion
# Example visualization
display(transformed_df)

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Set the colors as per user's preference
color_2020_2021 = "blue"
color_2021_2022 = "yellow"

# Define the necessary variables
school_attendance_df = pd.read_csv(
    "https://github.com/nogibjj/Jiechen_Li_Mini_6_MySQL/"
    "raw/main/School_Attendance_by_Student_Group_and_District__2021-2022.csv"
)

all_students_df = school_attendance_df[
    school_attendance_df["Student group"] == "All Students"
]
# Visualization of top 15 districts based on rate difference
subset_df = all_students_df.head(15)

# Set bar width and index for positioning of bars
bar_width = 0.35
index = np.arange(len(subset_df["District name"]))

# Create the bar chart

# Set up the figure and axes
fig, ax = plt.subplots(figsize=(14, 10))

# Plotting the attendance rates for each district with the specified colors
ax.bar(
    index,
    subset_df["2020-2021 attendance rate"],
    bar_width,
    label="2020-2021",
    alpha=0.8,
    color="blue",
)
ax.bar(
    index + bar_width,
    subset_df["2021-2022 attendance rate - year to date"],
    bar_width,
    label="2021-2022",
    alpha=0.8,
    color="yellow",
)

# Labeling and title
ax.set_xlabel("District Name")
ax.set_ylabel("Attendance Rate")
ax.set_title("Comparison of Attendance Rates for 2020-2021 and 2021-2022")
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(subset_df["District name"], rotation=90)
ax.legend()

# Display the plot
plt.tight_layout()
plt.show()
