from pyspark.sql import SparkSession

# Initialize a Spark session
#spark = SparkSession.builder.appName("GeoCountries").getOrCreate()
spark = SparkSession.builder.appName("BigMac").getOrCreate()

# Load the data from HDFS, select columns, and drop rows with missing values
#df_geo_countries = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/world-data-2023.csv", header=True)
df_bigmac = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/bigmac.csv", header=True)
df_bigmac.show()
#df_geo_countries = df_geo_countries.select("Country", "Latitude", "Longitude").dropna()

# Save the resulting DataFrame as a CSV file on your local machine
#df_geo_countries.write.csv("geo-countries.csv", header=True)

# Stop the Spark session
spark.stop()