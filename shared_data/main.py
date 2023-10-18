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

df_bigmac = df_bigmac.withColumn("date", when(col("date").isNotNull(), col("date").cast("date")))
df_bigmac = df_bigmac.withColumn("Year", year(col("date")))

for row in df_inflation.collect():
    for year in range(1970, 2023):
        if row[str(year)] == "":
            if str(year) == "1970":
                df_inflation = df_inflation.withColumn(str(year), 0)
            else:
                df_inflation = df_inflation.withColumn(str(year), row[str(year - 1)])

df_inflation.show(10)

def group_by_name_big_mac_and_agg_by_year(df):
    df = df.groupBy("name", "year").agg(sum("local_price").alias("local_price_sum"), sum("dollar_price").alias("dollar_price_sum"))
    return df

def merge_df_by_country_name(df1, df2):
    df = df1.join(df2, df1["Country"] == df2["name"], "inner").drop("name")

    df = df.withColumn("inflation_value", when(col("Year") == 1970, 0).otherwise(0))

    for year in range(1970, 2023):
        df = df.withColumn("inflation_value", when(col("Year") == year, col("inflation_value") + col(str(year))).otherwise(col("inflation_value")))
        df = df.drop(str(year))

    return df

def save_df_to_csv(df, path):
    df.coalesce(1).write.save(path, format='csv', mode='overwrite', header=True)

df_bigmac = group_by_name_big_mac_and_agg_by_year(df_bigmac)
df_result = merge_df_by_country_name(df_inflation, df_bigmac)
save_df_to_csv(df_result, "hdfs://namenode:9000/data/openbeer/data/output/csv_inflation_bigmac.csv")

df_inflation.show(10)
df_bigmac.show(10)

spark.stop()

