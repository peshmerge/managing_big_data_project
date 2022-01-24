from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from shapely import wkt
from shapely.geometry import shape
import json
import urllib.request

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
#spark.sparkContext.addPyFile("dependencies.zip")

DATA_PATH_2019 = '/user/s2665220/project/data/input/2019-*/*_fixed_*.parquet'
DATA_PATH_2021 = '/user/s2665220/project/data/input/2021-*/*_fixed_*.parquet'

GEOJSON_PATH = 'https://datahub.io/core/geo-countries/r/countries.geojson'

OUTPUT_DIR = '/user/s2665220/project/data/input/joined_data'

df_2019 = spark.read.parquet(DATA_PATH_2019)
df_2019 = df_2019.selectExpr("tile", "avg_d_kbps as 2019_avg_d_kbps", "avg_u_kbps as 2019_avg_u_kbps", "avg_lat_ms as 2019_avg_lat_ms", "tests as 2019_tests", "devices as 2019_devices")

df_2021 = spark.read.parquet(DATA_PATH_2021)
df_2021 = df_2021.selectExpr("tile", "avg_d_kbps as 2021_avg_d_kbps", "avg_u_kbps as 2021_avg_u_kbps", "avg_lat_ms as 2021_avg_lat_ms", "tests as 2021_tests", "devices as 2021_devices")

spark.sql("create database ookla")

df_2019.repartition(20, "tile").write.bucketBy(20, "tile").sortBy("tile").mode("overwrite").format("parquet").saveAsTable("ookla.table2019")

df_2021.repartition(20, "tile").write.bucketBy(20, "tile").sortBy("tile").mode("overwrite").format("parquet").saveAsTable("ookla.table2021")

df_joined = df_2021.join(df_2019, ["tile"])

df_joined = df_joined.groupBy("tile").agg(
        F.collect_set("2021_avg_d_kbps").alias("avg_d_kbps_21"), 
        F.collect_set("2021_avg_u_kbps").alias("avg_u_kbps_21"), 
        F.collect_set("2021_avg_lat_ms").alias("avg_lat_ms_21"),
        F.collect_set("2021_tests").alias("tests_21"), F.collect_set("2021_devices").alias("devices_21"), 
        F.collect_set("2019_avg_d_kbps").alias("avg_d_kbps_19"), 
        F.collect_set("2019_avg_u_kbps").alias("avg_u_kbps_19"), 
        F.collect_set("2019_avg_lat_ms").alias("avg_lat_ms_19"), 
        F.collect_set("2019_tests").alias("tests_19"), F.collect_set("2019_devices").alias("devices_19"))

with urllib.request.urlopen(GEOJSON_PATH) as url:
    gj = json.loads(url.read().decode())

country_polys = {}
for feature in gj['features']:
    country = feature['properties']['ADMIN']
    geometry = feature['geometry']
    country_polys[country] = shape(geometry)

def get_country(polygon):
    polygon = wkt.loads(polygon)
    for count in country_polys:
        if polygon.within(country_polys[count]):
            return count
    return 'NA'

country_udf = F.udf(get_country, T.StringType())

df_joined = df_joined.withColumn("country", country_udf(df_joined["tile"]))

df_joined.write.mode("overwrite").parquet(OUTPUT_DIR)
