from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, current_timestamp, lit, trim
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os


"""
    - BREWERIES TO SILVER
    In this notebook, we'll process Open Breweries DB data in JSON format from the bronze zone in our Azure Data Lake. 
    With our Spark cluster set up, the master node will handle job initialization. 
    The goal of this step is to transform the raw data into delta format and write it to the silver layer.

    You can monitor the Spark UI at http://localhost:8081. 
"""

# Let's define some variable to access the datalake with Store Account Credentials
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_TOKEN = os.environ["AZURE_TOKEN"]

builder = SparkSession.builder \
            .master("spark://spark:7077") \
            .appName("Open Brewery DB raw data processing")

spark = configure_spark_with_delta_pip(builder) \
            .getOrCreate()

# Container names
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

"""
    Many breweries lack values for address_2 and address_3. To simplify our curated data,
    we will use the coalesce function to select the most relevant address and consolidate
    it into a single address column in the final DataFrame. Additionally, we apply the `trim`
    function to remove any leading or trailing spaces from the address fields, especially 
    in the country column, to prevent creating multiple partitions for the same country
    due to extra spaces. Finally, an updated_at column will be added to capture the processing timestamp. 
"""

df_transformed = df.select(
    trim(col("id")).alias("id"),
    trim(col("name")).alias("name"),
    trim(col("brewery_type")).alias("brewery_type"),
    coalesce(trim(col("address_1")), trim(col("address_2")), trim(col("address_3"))).alias("address"),
    trim(col("city")).alias("city"),
    trim(col("state")).alias("state"),
    trim(col("postal_code")).alias("postal_code"),
    coalesce(trim(col("country")), lit("Unknown")).alias("country"),
    col("longitude"),
    col("latitude"),
    trim(col("phone")).alias("phone"),
    trim(col("website_url")).alias("website_url")
).withColumn("updated_at", current_timestamp())

"""
    We are also removing the state_province and street columns because the data they contain is redundant. 
    The state_province information is already included in the state column, 
    and the details in the street column are duplicated in the newly created address column, 
    which consolidates all relevant address information.
"""

# Check if the delta table exists in the silver container. If it doesn't, create it.
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

"""
    Now that we have the silver data available, I initially planned to run OPTIMIZE to compact the delta files,
    reducing the number of small files and improving metadata efficiency. However, after benchmarking the pipeline performance,
    I found that it wasn't worthwhile, as we are dealing with a small amount of data. Therefore, OPTIMIZE does not significantly 
    enhance read performance for the gold layer. Following this, we will still apply Z-ORDER to organize the data by frequently queried 
    columns, which will improve read performance when accessing the gold layer.
"""

#spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (brewery_type)")

