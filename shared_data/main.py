from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, when, sum, lit, avg, first, lag, corr

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

    df = df.groupBy("Country", "Year", "Series Name").agg(
        first("dollar_price").alias("dollar_price"),
        avg("dollar_price").alias("dollar_price_avg"),
        avg("inflation_value").alias("inflation_value"),
    )

    filtered_df = df.filter((df["Series Name"] == "Headline Consumer Price Inflation") |
                            (df["Series Name"] == "Food Consumer Price Inflation") |
                            (df["Series Name"] == "Energy Consumer Price Inflation"))

    result_df = filtered_df.groupBy("Country", "Year").agg(
        avg("dollar_price").alias("dollar_price_avg"),
        avg(when(df["Series Name"] == "Headline Consumer Price Inflation", df["inflation_value"])).alias("global_inflation_avg"),
        avg(when(df["Series Name"] == "Food Consumer Price Inflation", df["inflation_value"])).alias("food_inflation_avg"),
        avg(when(df["Series Name"] == "Energy Consumer Price Inflation", df["inflation_value"])).alias("energy_inflation_avg")
    )

    window_spec = Window.partitionBy("Country").orderBy("Year")
    result_df = result_df.withColumn("lag_price", lag("dollar_price_avg").over(window_spec))
    result_df = result_df.withColumn("bigmac_inflation",
                                 when(col("Year") == 1970, 0)
                                 .otherwise((col("dollar_price_avg") - col("lag_price")) / col("lag_price") * 100)
                                 )
    result_df = result_df.select("Country", "Year", "dollar_price_avg", "bigmac_inflation", "global_inflation_avg", "food_inflation_avg", "energy_inflation_avg")

    result_df = result_df.withColumn("diff", col("global_inflation_avg") - col("bigmac_inflation"))

    result_df = result_df.withColumn("correlation", corr("bigmac_inflation", "global_inflation_avg").over(window_spec))


    return result_df

def agg_for_all_years(df):

    agg_result = df.groupBy("Year").agg(
        avg("global_inflation_avg").alias("global_inflation_avg"),
        avg("dollar_price_avg").alias("dollar_price_avg"),
        avg("bigmac_inflation").alias("bigmac_inflation"),
        avg("correlation").alias("correlation"),
        avg("diff").alias("diff"),
    )



    return agg_result.sort("Year")



def save_df_to_csv(df, path):
    df.coalesce(1).write.save(path, format='csv', mode='overwrite', header=True)

df_result = merge_df_by_country_name(df_inflation, df_bigmac)

save_df_to_csv(df_result, "hdfs://namenode:9000/data/openbeer/data/output/csv_inflation_bigmac.csv")

agg_result = agg_for_all_years(df_result)
agg_result.show(10)
save_df_to_csv(agg_result, "hdfs://namenode:9000/data/openbeer/data/output/csv_agg_inflation_bigmac.csv")

df_mcdo = df_mcdo.withColumnRenamed("Fiscal Year / Year", "Year").withColumnRenamed("McDonald's Revenue", "McDonalds_Revenue")
save_df_to_csv(df_mcdo, "hdfs://namenode:9000/data/openbeer/data/output/csv_mcdo.csv")

spark.stop()
