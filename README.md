# Spark-Practical-Task

1. Validate Restaurant Data
Objective: Ensure no missing or invalid latitude and longitude values.
Approach: A simple script checks for null values in the lat and lng columns.
2. Enrich Restaurant Data
Objective: Use OpenCage Geocoding API to add formatted addresses based on lat and lng.
Outcome: Address column (address) is added to the restaurant data.
3. Generate Geohash
Objective: Compute a 4-character geohash for each restaurant's latitude and longitude.
Library Used: pygeohash.
4. Combine Weather Data
Objective: Merge restaurant data with weather data using the 4-character geohash.
Challenge: Ensure no data duplication and idempotency of the operation.
Outcome: A combined dataset (restaurant_and_weather_merged.csv) enriched with weather details.
5. Save to Parquet
Objective: Save the combined data in Parquet format with partitioning for efficient access.
Partitioning Key: franchise_name.
