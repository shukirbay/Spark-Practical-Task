# Step 1: Validate Restaurant Data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Validate Restaurant Data") \
    .getOrCreate()

# Load the restaurant data
restaurant_path = "C:/Users/daure/Desktop/restaurant/enriched_with_geohash.csv"
restaurant_df = spark.read.csv(restaurant_path, header=True, inferSchema=True)

# Check for null values in latitude and longitude
invalid_data_df = restaurant_df.filter((col("lat").isNull()) | (col("lng").isNull()))
invalid_data_df.show()

# Save invalid data if needed
if invalid_data_df.count() > 0:
    invalid_data_df.write.csv("C:/Users/daure/Desktop/restaurant/invalid_restaurant_data.csv", header=True)

# Continue with valid data
valid_data_df = restaurant_df.filter((col("lat").isNotNull()) & (col("lng").isNotNull()))

# Step 2: Combine Weather Data
# Paths to weather files
weather_files = [
    "C:/Users/daure/Desktop/weather/august_2017_weather_with_geohash.csv",
    "C:/Users/daure/Desktop/weather/october_2016_weather_with_geohash.csv",
    "C:/Users/daure/Desktop/weather/september_2017_weather_with_geohash.csv"
]

# Load and union all weather files
weather_dfs = [spark.read.csv(file, header=True, inferSchema=True) for file in weather_files]
weather_combined_df = weather_dfs[0].unionByName(weather_dfs[1]).unionByName(weather_dfs[2])

# Save the combined weather data
weather_combined_path = "C:/Users/daure/Desktop/weather/weather_combined_geohash.csv"
weather_combined_df.write.csv(weather_combined_path, header=True)

#Step 3: Normalize Geohash
# Normalize geohash in both datasets
valid_data_df = valid_data_df.withColumn("geohash", col("geohash").cast("string").lower())
weather_combined_df = weather_combined_df.withColumn("geohash", col("geohash").cast("string").lower())

#Step 4: Merge Restaurant and Weather Data
# Join restaurant data with weather data
merged_df = valid_data_df.join(weather_combined_df, on="geohash", how="left")

# Save the merged data
merged_path = "C:/Users/daure/Desktop/restaurant_and_weather_merged.csv"
merged_df.write.csv(merged_path, header=True)

#Step 5: Save as Parquet with Partitioning
# Save the merged data as Parquet, partitioned by franchise_name
parquet_path = "C:/Users/daure/Desktop/restaurant_weather_parquet/"
merged_df.write.mode("overwrite").partitionBy("franchise_name").parquet(parquet_path)
