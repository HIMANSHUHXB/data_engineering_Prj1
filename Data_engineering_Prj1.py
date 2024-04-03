# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC 1)This notebook will show you how to create , query a table or DataFrame that you uploaded to DBFS .                                                
# MAGIC  2) Store the tables in databse .                                                                                                                   
# MAGIC   3) Call the tables and join them using pyspark and save the result as a final table.                                                    

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/cus_ord2_csv-1.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)
#save the dataframe as a table in database(default)
df.write.mode("overwrite").saveAsTable("default.customer2")

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/order2.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)
#save the dataframe as a table in database(default)
df.write.mode("overwrite").saveAsTable("default.order2")

# COMMAND ----------

# MAGIC %sql
# MAGIC --check the tables if they are saved in database (default)
# MAGIC select * from default.customer2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.order2

# COMMAND ----------

# MAGIC %md
# MAGIC #Starting of curation to call the tables from databases and use the tables

# COMMAND ----------

#Import the modules:
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

# COMMAND ----------

#Create a Spark session:
spark = SparkSession.builder.appName("first_curation").enableHiveSupport().getOrCreate()

# COMMAND ----------

databs_df=spark.sql('show databases')
display(databs_df)

# COMMAND ----------

import pyspark.sql.functions as F

cus_df=spark.read.table('customer2')
cus_final_df=cus_df.filter(F.col("CustomerID").isNotNull())
cus_final_df.show()

# COMMAND ----------

ord_df=spark.read.table('order2')
ord_df.show()

# COMMAND ----------

#join the two dfs as a final df 

temp1_df=cus_final_df.join(ord_df, 
                       cus_final_df.CustomerID==ord_df.CustomerID, 
                        "left").orderBy(cus_df.CustomerName)

# "cus_final_df.CustomerName","cus_final_df.Address", "cus_final_df.City" "cus_final_df. PostalCode")
display(temp1_df)

# COMMAND ----------

#select few columns you want from the temp1_df.As temp1_df contains all the columns from both cus_final_df and ord_df
selected_df = temp1_df.select("spark_catalog.default.customer2.CustomerID","spark_catalog.default.customer2.CustomerName","spark_catalog.default.customer2.Address","spark_catalog.default.customer2.City","spark_catalog.default.customer2.PostalCode","spark_catalog.default.order2.orderID")
display(selected_df)


# COMMAND ----------


