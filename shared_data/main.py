from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("GeoCountries").getOrCreate()

# Load the data from HDFS, select columns, and drop rows with missing values
df_geo_countries = spark.read.csv("hdfs://namenode:9000/data/openbeer/datasets/world-data-2023.csv", header=True)
df_geo_countries = df_geo_countries.select("Country", "Latitude", "Longitude").dropna()

# Save the resulting DataFrame as a CSV file on your local machine
df_geo_countries.write.csv("geo-countries.csv", header=True)

# Stop the Spark session
spark.stop()