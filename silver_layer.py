# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data acccess using app

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azstorageadls.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azstorageadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azstorageadls.dfs.core.windows.net", "7f765595-940b-466b-8e3d-f725ecff5ccb")
spark.conf.set("fs.azure.account.oauth2.client.secret.azstorageadls.dfs.core.windows.net", "Eo48Q~1JKVrCqd7TW2A-uiaFg8c7VSk6in4I7dc.")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azstorageadls.dfs.core.windows.net", "https://login.microsoftonline.com/93e98254-0c96-4bcc-b221-6142c39624e1/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the data

# COMMAND ----------

df_calender = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_customers = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_Product_Categories = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_Product_Subcategories = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

# COMMAND ----------

df_Products = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_Returns = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_Sales = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_Territories = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("abfss://bronze@azstorageadls.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calender

# COMMAND ----------

# DBTITLE 1,Cell 18
df_calender = df_calender.withColumn('Month',month(col('Date'))).withColumn('year',year(col('Date')))
df_calender.display()

# COMMAND ----------

df_calender.write.format('parquet').mode('append').option("path","abfss://silver@azstorageadls.dfs.core.windows.net/AdventureWorks_Calender").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customers

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers = df_customers.withColumn("FullName", concat_ws(" ", col("Prefix"), col("FirstName"), col("LastName")))
df_customers.display()

# COMMAND ----------

df_customers.write.format('parquet').mode('append').option("path","abfss://silver@azstorageadls.dfs.core.windows.net/AdventureWorks_Customers").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Product Subcategories

# COMMAND ----------

df_Product_Subcategories.write.format('parquet').mode('append').option("path","abfss://silver@azstorageadls.dfs.core.windows.net/AdventureWorks_Product_Subcategories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Products

# COMMAND ----------

df_Products.display()

# COMMAND ----------

df_Products = df_Products.withColumn('ProductSKU', split(col('ProductSKU'), '-').getItem(0)).withColumn('ProductName', split(col('ProductName'), '-').getItem(0))
df_Products.display()


# COMMAND ----------

df_Products.write.format('parquet').mode('append').option("path","abfss://silver@azstorageadls.dfs.core.windows.net/AdventureWorks_Products").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Returns

# COMMAND ----------

df_Returns.display()

# COMMAND ----------

df_Returns.write.format('parquet').mode('append').option("path","abfss://silver@azstorageadls.dfs.core.windows.net/AdventureWorks_Returns").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Territories

# COMMAND ----------

df_Territories.display()


# COMMAND ----------

df_Territories.write.format('parquet').mode('append').option("path","abfss://silver@azstorageadls.dfs.core.windows.net/AdventureWorks_Territories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

#converting StockDate to timestamp
df_Sales = df_Sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

#replacing S with T in OrderNumber
df_Sales = df_Sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

#adding a new column 'multiply' = OrderLineItem * OrderQuantity
df_Sales = df_Sales.withColumn('multiply', col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_Sales.display()

# COMMAND ----------

df_Sales.write.format('parquet').mode('append').option("path", "abfss://silver@azstorageadls.dfs.core.windows.net/AdventureWorks_Sales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales Analysis

# COMMAND ----------

#Analysis of total
df_Sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_Product_Categories.display()

# COMMAND ----------

df_Territories.display()