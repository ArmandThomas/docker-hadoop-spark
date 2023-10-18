from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, sum

spark = SparkSession.builder.appName("dataClean").getOrCreate()

df_geo_countries = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/world-data-2023.csv", header=True)
df_geo_countries = df_geo_countries.select("Country", "Latitude", 'Longitude"').dropna()

df_geo_countries = df_geo_countries.withColumnRenamed('Longitude"', "Longitude")

df_inflation = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/inflation.csv", header=True)
df_inflation = df_inflation.select(
    "Country Code",
    "Country",
    "1970", "1971", "1972", "1973", "1974", "1975", "1976", "1977", "1978", "1979", "1980", "1981", "1982", "1983",
    "1984", "1985", "1986", "1987", "1988", "1989", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998", "1999",
    "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014",
    "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"
).dropna()

df_bigmac = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/bigmac.csv", header=True)
df_bigmac = df_bigmac.select(
    "date", "name", "local_price", "dollar_price"
).dropna()

df_inflation = df_inflation.fillna(0)

for year in range(1970, 2023):
    df_inflation = df_inflation.withColumn(str(year), col(str(year)).cast("float"))

def save_inflation_and_bigmac_data(spark, df_inflation, df_bigmac, output_path):
    # Convert all relevant columns in the inflation dataframe to float
    inflation_columns = [str(year) for year in range(1970, 2023)]
    for year in inflation_columns:
        df_inflation = df_inflation.withColumn(year, col(year).cast("float"))

    # Convert the date column in the bigmac dataframe to a date type and then extract the year
    df_bigmac = df_bigmac.withColumn("Year", year(when(col("date").isNotNull(), col("date").cast("date"))))

    # Group the inflation data by year and calculate the sum for each year
    inflation_sum_columns = [sum(col(year)).alias(year) for year in inflation_columns]
    df_inflation_sum = df_inflation.groupBy().agg(*inflation_sum_columns)

    # Group the bigmac data by year and calculate the sum of "local_price" for each year
    df_bigmac_sum = df_bigmac.groupBy("Year").agg(sum("local_price").alias("Local_Price_Sum"))

    # Join the inflation and bigmac dataframes on the "Year" column
    result_df = df_inflation_sum.crossJoin(df_bigmac_sum)

    result_df.write.option("header", "true").csv(output_path)

save_inflation_and_bigmac_data(spark, df_inflation, df_bigmac, "hdfs://namenode:9000/data/openbeer/data/output/inflation_bigmac.csv")

spark.stop()
