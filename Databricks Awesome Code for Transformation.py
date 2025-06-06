# Databricks notebook source
spark

# COMMAND ----------

storage_account = "olistdatastorageaccdjain"
application_id = "b153323e-6f76-4b7b-9284-bb2e61126bf0"
directory_id = "7a9e5449-6e50-49e5-99d0-235a025527a2"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", "aV38Q~xzO3KOP_uDCGAJrW3UC9KeykENXyZhlam1")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

file_path = f"abfss://olistdata@{storage_account}.dfs.core.windows.net/bronze/olist_customers_dataset.csv"

customer_df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(file_path)
display(customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading the data

# COMMAND ----------

base_path = "abfss://olistdata@olistdatastorageaccdjain.dfs.core.windows.net/bronze/"
orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format("csv").option("header", "true").load(orders_path)
payments_df = spark.read.format("csv").option("header", "true").load(payments_path)
reviews_df = spark.read.format("csv").option("header", "true").load(reviews_path)
items_df = spark.read.format("csv").option("header", "true").load(items_path)
customers_df = spark.read.format("csv").option("header", "true").load(customers_path)
sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
products_df = spark.read.format("csv").option("header", "true").load(products_path)

# COMMAND ----------

geolocation_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading MongoDB data from Pymongo

# COMMAND ----------

!pip install pymongo

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "9zf78.h.filess.io"
database = "olistDataNoSQL_stillatjob"
port = "27018"
username = "olistDataNoSQL_stillatjob"
password = "e8144738413aebc8c528ef6c8b8c02c02db5c3d3"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

import pandas as pd
collection = mydatabase['product_categories']
mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

mongo_data.head(10)

# COMMAND ----------

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning the data

# COMMAND ----------

display(orders_df.count())
display(payments_df.count())
display(reviews_df.count())
display(items_df.count())
display(customers_df.count())
display(sellers_df.count())
display(geolocation_df.count())
display(products_df.count())

# COMMAND ----------

from pyspark.sql.functions import col,to_date,datediff,current_date,when
def clean_dataframe(df,name):
    print("Cleaning dataframe : ",name)
    return df.dropDuplicates().na.drop('all')
orders_df = clean_dataframe(orders_df,'Orders')
display(orders_df)
payments_df = clean_dataframe(payments_df,'Payments')
display(payments_df)
reviews_df = clean_dataframe(reviews_df,'Reviews')
display(reviews_df)
items_df = clean_dataframe(items_df,'Items')
display(items_df)
customers_df = clean_dataframe(customers_df,'Customers')
display(customers_df)
sellers_df = clean_dataframe(sellers_df,'Sellers')
display(sellers_df)
geolocation_df = clean_dataframe(geolocation_df,'Geolocation')
display(geolocation_df)
products_df = clean_dataframe(products_df,'Products')
display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change data type from string to date for Date columns

# COMMAND ----------

# Convert Data columns
orders_df = orders_df.withColumn('order_purchase_timestamp', to_date(col('order_purchase_timestamp'), 'yyyy-MM-dd HH:mm:ss')) \
                     .withColumn('order_approved_at', to_date(col('order_approved_at'), 'yyyy-MM-dd HH:mm:ss')) \
                     .withColumn('order_delivered_carrier_date', to_date(col('order_delivered_carrier_date'), 'yyyy-MM-dd HH:mm:ss')) \
                     .withColumn('order_delivered_customer_date', to_date(col('order_delivered_customer_date'), 'yyyy-MM-dd HH:mm:ss')) \
                     .withColumn('order_estimated_delivery_date', to_date(col('order_estimated_delivery_date'), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Some Analysis

# COMMAND ----------

orders_df = orders_df.withColumn('actual_delivery_time', datediff(col('order_delivered_customer_date'), col('order_purchase_timestamp')))
orders_df = orders_df.withColumn('estimated_delivery_time', datediff(col('order_estimated_delivery_date'), col('order_purchase_timestamp')))
orders_df = orders_df.withColumn('delay Time', col('actual_delivery_time') - col('estimated_delivery_time'))

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining Dataframes

# COMMAND ----------

# orders_df.printSchema()
# customers_df.printSchema()
# payments_df.printSchema()
items_df.printSchema()
# products_df.printSchema()
sellers_df.printSchema()

# COMMAND ----------

orders_customers_df = orders_df.join(customers_df,orders_df.customer_id == customers_df.customer_id,"left")
# display(orders_customers_df)
orders_payments_df = orders_customers_df.join(payments_df,orders_customers_df.order_id == payments_df.order_id,"left")
# display(orders_payments_df)
orders_items_df = orders_payments_df.join(items_df,"order_id","left")
# display(orders_items_df)
orders_items_products_df = orders_items_df.join(products_df,orders_items_df.product_id == products_df.product_id,"left")
# display(orders_items_products_df)
final_df = orders_items_products_df.join(sellers_df,orders_items_products_df.seller_id == sellers_df.seller_id,"left")
display(final_df)

# COMMAND ----------

final_df.count()

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)
mongo_spark_df = spark.createDataFrame(mongo_data)


# COMMAND ----------

final_df = final_df.join(mongo_spark_df,"product_category_name","left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
  columns = df .columns
  seen_columns = set()
  columns_to_drop = []
  for column in columns:
    if column in seen_columns:
      columns_to_drop.append(column)
    else:
      seen_columns.add(column)

  df_cleaned = df.drop(*columns_to_drop)
  return df_cleaned

final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastorageaccdjain.dfs.core.windows.net/silver")

# COMMAND ----------

