from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, sum, date_format, to_date
import os

# ---------------------------------------------------------------------------------------------------------------------------------
# Prepare -------------------------------------------------------------------------------------------------------------------------
PATH = 'hdfs://namenode:9000/raw_zone/fact/'
csv_file_path = PATH + 'danh_sach_sv_de.csv'
parquet_file_path = PATH + 'activity/'

student_schema = StructType([
    StructField("code", IntegerType(), True),
    StructField("fullname", StringType(), True)
])


# ---------------------------------------------------------------------------------------------------------------------------------
# Create Spark session ------------------------------------------------------------------------------------------------------------
spark = SparkSession.builder.appName('vdt2024').getOrCreate()


# ---------------------------------------------------------------------------------------------------------------------------------
# Read CSV file -------------------------------------------------------------------------------------------------------------------
student_df = spark.read.csv(csv_file_path, header=False, schema=student_schema)

print("Student preview:")
student_df.limit(5).show()

# Read Parquet file----------------------------------------------------------------------------------------------------------------
activity_df = spark.read.parquet(parquet_file_path)

print("Activity preview:")
activity_df.limit(5).show()


# ---------------------------------------------------------------------------------------------------------------------------------
# Process dataframes --------------------------------------------------------------------------------------------------------------
raw_df = activity_df.join(student_df, activity_df['student_code'] == student_df['code'], 'outer')
        
# Transform the date column -------------------------------------------------------------------------------------------------------
transformed_df = raw_df.withColumn('date', date_format(to_date(col('timestamp'), 'M/dd/yyyy'), 'yyyyMMdd'))
transformed_df = transformed_df.withColumnRenamed("fullname", "student_name")


# Transform the date column -------------------------------------------------------------------------------------------------------
transformed_df.createOrReplaceTempView("temp_view")
query = """
    SELECT date, student_code, student_name, activity, int(sum(numberOfFile)) as totalFile
    FROM temp_view
    GROUP BY date, student_code, student_name, activity
"""

# Execute the SQL query
output_df = spark.sql(query)

print("Result preview:")
output_df.limit(5).show()


# ---------------------------------------------------------------------------------------------------------------------------------
# Store output --------------------------------------------------------------------------------------------------------------------
output_df.write.option("header",True).option("delimiter",",").mode("overwrite").csv("output")


# ---------------------------------------------------------------------------------------------------------------------------------
# Stop Spark session --------------------------------------------------------------------------------------------------------------
spark.stop()