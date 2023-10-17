from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, year, corr

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

def calculate_correlation_all_countries():
    for year in range(1970, 2023):
        df_inflation = df_inflation.withColumn(str(year), expr(f"CAST({year} AS DOUBLE)"))

    inflation_sum_df = df_inflation.groupBy("Country", "Country Code").sum(*[str(year) for year in range(1970, 2023)])

    bigmac_sum_df = df_bigmac.withColumn("year", year("date"))
    bigmac_sum_df = bigmac_sum_df.groupBy("year").sum("dollar_price")
    bigmac_sum_df = bigmac_sum_df.withColumnRenamed("sum(dollar_price)", "bigmac_price_sum")

    result_df = inflation_sum_df.join(bigmac_sum_df, inflation_sum_df["year"] == bigmac_sum_df["year"], "inner")

    correlation = result_df.select(corr("inflation_sum_1970", "bigmac_price_sum").alias("correlation"))

    correlation.write.csv("/app/results/correlation_all_countries.csv", header=True, mode="overwrite")

def agg_inf_by_year_and_bigmac():
    inflation_sum_df = df_inflation.groupBy("Country", "Country Code").sum(
        *[str(year) for year in range(1970, 2023)]
    )

    bigmac_sum_df = df_bigmac.withColumn("year", year("date"))

    bigmac_sum_df = bigmac_sum_df.groupBy("name", "year").sum("dollar_price")

    bigmac_sum_df = bigmac_sum_df.withColumnRenamed("sum(dollar_price)", "bigmac_price_sum")

    result_df = inflation_sum_df.join(bigmac_sum_df, [inflation_sum_df["Country"] == bigmac_sum_df["name"],
                                                      inflation_sum_df["year"] == bigmac_sum_df["year"]], "inner")

    result_df = result_df.join(df_geo_countries, "Country", "inner")

    result_df.write.csv("/app/results/agg_inf_by_year_and_bigmac.csv", header=True, mode="overwrite")

agg_inf_by_year_and_bigmac()
calculate_correlation_all_countries()

spark.stop()