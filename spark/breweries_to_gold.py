
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os


"""
    - BREWERIES TO GOLD
    In this notebook, we'll process Open Breweries DB data in delta format from the silver zone in our Azure Data Lake. 
    With our Spark cluster set up, the master node will handle job initialization. 
    The goal of this step is to aggregate the curated data into delta format and write it to the Gold layer.

    You can monitor the Spark UI at http://localhost:8081. 
"""

# Let's define some variable to access the datalake with Store Account Credentials
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_TOKEN = os.environ["AZURE_TOKEN"]

builder = SparkSession.builder \
            .appName("Open Brewery DB Silver data aggregation")

spark = configure_spark_with_delta_pip(builder) \
            .getOrCreate()

# Container names
SILVER_CONTAINER="silver"
GOLD_CONTAINER="gold"

# Silver container (source)
silver_path = f"wasbs://{SILVER_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/"
# Gold container (target)
gold_path = f"wasbs://{GOLD_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/"

# Reading silver data partitioned by country
df = spark.read \
        .format("delta") \
        .load(silver_path)

# Let's aggregate data and count the number of breweries by type and country
df_transformed = df.select(
    col("country"),
    col("brewery_type"),
    col("id")
).groupBy(col("brewery_type"), col("country")
).agg(count("id").alias("brewery_count"))

# Let's check if delta table exists in gold container. If not, create it.
is_delta = DeltaTable.isDeltaTable(spark, gold_path)
if not is_delta:
    print(f"No delta found in {gold_path}. Writing full table...")
    
    df_transformed.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_path)
else:
    print(f"Delta found in {gold_path}. Merging silver and gold data...")
    
    df_target = DeltaTable.forPath(spark, gold_path)

    # merging silver with gold
    df_target.alias("target").merge(
        df_transformed.alias("source"),
        "target.brewery_type = source.brewery_type AND target.country = source.country") \
    .whenNotMatchedInsertAll() \
    .whenMatchedUpdateAll() \
    .execute()

# Now that we have the gold data available, we will run OPTIMIZE and Z-ORDER as well

#spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (brewery_type, country)")

spark.stop()

