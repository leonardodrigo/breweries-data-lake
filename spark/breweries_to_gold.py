
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os


"""
    - BREWERIES TO GOLD
    In this notebook, we'll process Open Breweries DB data in Delta format from the silver zone in our Azure Data Lake. 
    With our Spark cluster set up, the master node will handle job initialization. 
    The goal of this step is to aggregate the curated data into Delta format and write it to the Gold layer.

    You can monitor the Spark UI at http://localhost:8081. 
"""

# Let's define some variable to access the datalake with Store Account Credentials
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_TOKEN = os.environ["AZURE_TOKEN"]

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
            .appName("Open Brewery DB Silver data aggregation") \
            .config(conf=conf)

spark = configure_spark_with_delta_pip(builder) \
            .config(conf=conf) \
            .getOrCreate()

 # Containers name
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
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (brewery_type, country)")

spark.stop()
