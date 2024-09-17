import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os


"""
    - BREWERIES TO SILVER
    In this notebook, we'll process Open Breweries DB data in JSON format from the bronze zone in our Azure Data Lake. 
    With our Spark cluster set up, the master node will handle job initialization. 
    The goal of this step is to transform the raw data into Delta format and write it to the silver layer.

    You can monitor the Spark UI at http://localhost:8081. 
"""

# Let's define some variable to access the datalake with Store Account Credentials
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_TOKEN = os.environ["AZURE_TOKEN"]

print(AZURE_STORAGE_ACCOUNT)
print(AZURE_TOKEN)

# Since our Spark Cluster needs to access Azure, we must include some jar files in configuration
# - spark.sql.repl.eagerEval.enabled**: Enable eager evaluation for notebooks
# - park.sql.repl.eagerEval.maxNumRows**: Default number of rows

conf = SparkConf() \
            .set("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
            .set("spark.hadoop.fs.azure.account.key." + AZURE_STORAGE_ACCOUNT + ".blob.core.windows.net", f"{AZURE_TOKEN}") \
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .set("spark.sql.repl.eagerEval.enabled", True) \
            .set("spark.sql.repl.eagerEval.maxNumRows", 10)

builder = SparkSession.builder \
            .master("spark://spark:7077") \
            .appName("Open Brewery DB raw data processing") \
            .config(conf=conf)

spark = configure_spark_with_delta_pip(builder) \
            .config(conf=conf) \
            .getOrCreate()

 # Containers name
BRONZE_CONTAINER="bronze"
SILVER_CONTAINER="silver"

# Bronze container (source)
bronze_path = f"wasbs://{BRONZE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/"
# Silver container (target)
silver_path = f"wasbs://{SILVER_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/"

# Since we are dealing with JSON files, let's perform some standardizations and improvements.
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("brewery_type", StringType(), False),
    StructField("address_1", StringType(), False),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("city", StringType(), False),
    StructField("state_province", StringType(), False),
    StructField("postal_code", StringType(), False),
    StructField("country", StringType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("state", StringType(), False),
    StructField("street", StringType(), False)
])

df = spark.read \
        .format("json") \
        .schema(schema) \
        .load(bronze_path) \
        .distinct()

# Many breweries lack values for address_2 and address_3, so to simplify our curated data, 
# we will use coalesce to select the most relevant address and consolidate it into a single address column in the final dataframe.
# In addition to that, updated_at column will be added in the end of dataframe to hold the processing timestamp.
df_transformed = df.select(
    col("id"),
    col("name"),
    col("brewery_type"),
    coalesce(col("address_1"), col("address_2"), col("address_3")).alias("address"),
    col("city"),
    col("state"),
    col("postal_code"),
    coalesce(col("country"), lit("Unknown")).alias("country"),
    col("longitude"),
    col("latitude"),
    col("phone"),
    col("website_url")
).withColumn("updated_at", current_timestamp())

# We are removing the tate_province and street columns too because the data they contain is redundant. 
# The state_province information is already included in the state column, 
# and the street column's details are duplicated in the newly created address column, 
# which consolidates all relevant address information.

# Let's check if delta table exists in silver container. If not, create it.
is_delta = DeltaTable.isDeltaTable(spark, silver_path)
if not is_delta:
    print(f"No delta found in {silver_path}. Writing full table...")
    
    df_transformed.write \
        .format("delta") \
        .partitionBy("country") \
        .mode("overwrite") \
        .save(silver_path)
else:
    print(f"Delta found in {silver_path}. Merging bronze and silver data...")
    
    df_target = DeltaTable.forPath(spark, silver_path)

    # merging bronze with silver
    df_target.alias("target").merge(
        df_transformed.alias("source"),
        "target.id = source.id") \
    .whenNotMatchedInsertAll() \
    .whenMatchedUpdateAll() \
    .execute()

# Now that we have the silver data available, we will run **OPTIMIZE** to compact the delta files, 
# reducing the number of small files and improving metadata efficiency. 
# Following that, we will apply **Z-ORDER** to organize the data by frequently queried columns, 
# which will enhance read performance when accessing the gold layer.
#spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (brewery_type)")

