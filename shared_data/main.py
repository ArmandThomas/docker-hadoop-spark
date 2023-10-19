from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, when, sum, lit, avg

spark = SparkSession.builder.appName("dataClean").getOrCreate()

df_geo_countries = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/world-data-2023.csv", header=True)
df_geo_countries = df_geo_countries.select("Country", "Latitude", 'Longitude"').dropna()

df_geo_countries = df_geo_countries.withColumnRenamed('Longitude"', "Longitude")

df_inflation = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/inflation.csv", header=True)
df_inflation = df_inflation.select(
    "Country Code",
    "Country",
    "Series Name","1970", "1971", "1972", "1973", "1974", "1975", "1976", "1977", "1978", "1979", "1980", "1981", "1982", "1983",
    "1984", "1985", "1986", "1987", "1988", "1989", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998", "1999",
    "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014",
    "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"
).dropna()

type = {
 "Energy Consumer Price Inflation" : "energy",
 "Food Consumer Price Inflation" : "food",
 "Headline Consumer Price Inflation" : "headline",
}

df_inflation = df_inflation.withColumn("Series Name", when(col("Series Name").isNotNull(), type[col("Series Name")]).otherwise(col("Series Name")))

df_bigmac = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/bigmac.csv", header=True)
df_bigmac = df_bigmac.select(
    "date", "name", "local_price", "dollar_price"
).dropna()

df_bigmac = df_bigmac.withColumn("date", when(col("date").isNotNull(), col("date").cast("date")))
df_bigmac = df_bigmac.withColumn("Year", year(col("date")))

df_mcdo = spark.read.csv("hdfs://namenode:9000/data/openbeer/data/input/mcdo.csv", header=True)
df_mcdo = df_mcdo.select(
    "McDonald's Revenue", "Fiscal Year / Year"
).dropna()

df_mcdo = df_mcdo.withColumn("McDonald's Revenue", when(col("McDonald's Revenue").isNotNull(), col("McDonald's Revenue").substr(2, 4).cast("float") * 1000000000))

for row in df_inflation.collect():
    for year in range(1970, 2023):
        if row[str(year)] == "":
            if str(year) == "1970":
                df_inflation = df_inflation.withColumn(str(year), 0)
            else:
                df_inflation = df_inflation.withColumn(str(year), row[str(year - 1)])



def merge_df_by_country_name(df1, df2, group = True):
    df = df1.join(df2, df1["Country"] == df2["name"], "inner").drop("name")

    df = df.withColumn("inflation_value",  lit(0))

    for year in range(1970, 2023):
        df = df.withColumn("inflation_value", when(col("Year") == year, col(str(year))).otherwise(col("inflation_value")))
        df = df.drop(str(year))


   df = df.groupBy("Country", "Year").agg(
         avg("dollar_price").alias("dollar_price_avg"),
         avg("inflation_value").alias("inflation_value_avg"),
         when(col("Series Name") == "energy", col("inflation_value")).otherwise(lit(0)).alias("energy_inflation_value"),
         when(col("Series Name") == "food", col("inflation_value")).otherwise(lit(0)).alias("food_inflation_value"),
   )

    return df

def agg_for_all_years(df):

    agg_result = df.groupBy("Year").agg(
        avg("inflation_value").alias("inflation_value_sum"),
        avg("dollar_price_avg").alias("dollar_price_avg")
    )

    return agg_result.sort("Year")



def save_df_to_csv(df, path):
    df.coalesce(1).write.save(path, format='csv', mode='overwrite', header=True)

df_copy_result = merge_df_by_country_name(df_inflation, df_bigmac, False)

save_df_to_csv(df_copy_result, "hdfs://namenode:9000/data/openbeer/data/output/csv_inflation_bigmac_copy.csv")

df_result = merge_df_by_country_name(df_inflation, df_bigmac)

save_df_to_csv(df_result, "hdfs://namenode:9000/data/openbeer/data/output/csv_inflation_bigmac.csv")

agg_result = agg_for_all_years(df_result)
agg_result.show(10)
save_df_to_csv(agg_result, "hdfs://namenode:9000/data/openbeer/data/output/csv_agg_inflation_bigmac.csv")

df_mcdo = df_mcdo.withColumnRenamed("Fiscal Year / Year", "Year").withColumnRenamed("McDonald's Revenue", "McDonalds_Revenue")
save_df_to_csv(df_mcdo, "hdfs://namenode:9000/data/openbeer/data/output/csv_mcdo.csv")

spark.stop()
