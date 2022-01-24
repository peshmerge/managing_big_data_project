from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sklearn.neighbors import BallTree, DistanceMetric
import numpy as np
import shapely.wkt
import json
import urllib.request
import geopandas as gpd

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")


DATA_PATH_2019 = '/user/s2665220/project/data/input/2019-*/*_fixed_*.parquet'
DATA_PATH_2021 = '/user/s2665220/project/data/input/2021-*/*_fixed_*.parquet'

CITY_PATH = '/user/s2665220/project/data/allCountries.txt'
GEOJSON_PATH = 'https://datahub.io/core/geo-countries/r/countries.geojson'

OUTPUT_DIR = '/user/s2665220/project/data/input/joined_data_full'
INTER_MAIN_DIR = '/user/s2665220/project/data/input/temp/df'
INTER_CITY_DIR = '/user/s2665220/project/data/input/temp/city'

df_2019 = spark.read.parquet(DATA_PATH_2019)
df_2019 = df_2019.selectExpr("tile", "avg_d_kbps as 2019_avg_d_kbps", "avg_u_kbps as 2019_avg_u_kbps", "avg_lat_ms as 2019_avg_lat_ms", "tests as 2019_tests", "devices as 2019_devices")

df_2021 = spark.read.parquet(DATA_PATH_2021)
df_2021 = df_2021.selectExpr("tile", "avg_d_kbps as 2021_avg_d_kbps", "avg_u_kbps as 2021_avg_u_kbps", "avg_lat_ms as 2021_avg_lat_ms", "tests as 2021_tests", "devices as 2021_devices")

df_joined = df_2021.join(df_2019, "tile")

df_joined = df_joined.groupBy("tile").agg(
        F.collect_set("2021_avg_d_kbps").alias("avg_d_kbps_21"), 
        F.collect_set("2021_avg_u_kbps").alias("avg_u_kbps_21"), 
        F.collect_set("2021_avg_lat_ms").alias("avg_lat_ms_21"),
        F.collect_set("2021_tests").alias("tests_21"), F.collect_set("2021_devices").alias("devices_21"), 
        F.collect_set("2019_avg_d_kbps").alias("avg_d_kbps_19"), 
        F.collect_set("2019_avg_u_kbps").alias("avg_u_kbps_19"), 
        F.collect_set("2019_avg_lat_ms").alias("avg_lat_ms_19"), 
        F.collect_set("2019_tests").alias("tests_19"), F.collect_set("2019_devices").alias("devices_19"))

coord_udf = F.udf(lambda x: np.radians([shapely.wkt.loads(x).centroid.y, shapely.wkt.loads(x).centroid.x]).tolist(), T.ArrayType(T.DoubleType()))
df_joined = df_joined.withColumn("coord_df", coord_udf(df_joined["tile"]))

df_joined.repartition(60).write.mode("overwrite").parquet(INTER_MAIN_DIR)

spark.catalog.clearCache()
del df_joined

schema = T.StructType() \
        .add("id", T.StringType(), True) \
        .add("city", T.StringType(), True) \
        .add("x1", T.StringType(), True) \
        .add("x2", T.StringType(), True) \
        .add("latitude", T.DoubleType(), True) \
        .add("longitude", T.DoubleType(), True) \
        .add("x3", T.StringType(), True) \
        .add("x4", T.StringType(), True) \
        .add("x5", T.StringType(), True) \
        .add("x6", T.StringType(), True) \
        .add("x7", T.StringType(), True) \
        .add("x8", T.StringType(), True) \
        .add("x9", T.StringType(), True) \
        .add("x10", T.StringType(), True) \
        .add("x11", T.StringType(), True) \
        .add("x12", T.StringType(), True) \
        .add("x13", T.StringType(), True) \
        .add("x14", T.StringType(), True)

city_df = spark.read.options(delimiter='\t').option("header", False).schema(schema).csv(CITY_PATH)
city_df = city_df.select("city", "latitude", "longitude")

city_udf = F.udf(lambda lat,lon: np.radians([lat, lon]).tolist(), T.ArrayType(T.DoubleType()))
city_df = city_df.withColumn("coord", city_udf(city_df['latitude'], city_df['longitude']))
city_df = city_df.select("city", "coord").withColumn("id", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))-1)

with urllib.request.urlopen(GEOJSON_PATH) as url:
    gj = json.loads(url.read().decode())

geo_country = gpd.GeoDataFrame.from_features(gj["features"])

def get_country(p):
    ind = geo_country.geometry.sindex.query(shapely.wkt.loads(p), "within")
    if len(ind) == 0:
        return 'N/A'
    else:
        return geo_country['ADMIN'][ind[0]]

city_udf = F.udf(lambda p: get_country(p), T.StringType())
#city_df.repartition(50).write.mode("overwrite").parquet(INTER_CITY_DIR)
#city_df.repartition(300, "id").write.bucketBy(300, "id").sortBy("id").mode("overwrite").format("parquet").saveAsTable("ookla.table2021")

#city_list = [r["coord"] for r in city_df.select("coord").collect()]
#spark.catalog.clearCache()
#del city_df

#city_df = spark.read.parquet(INTER_CITY_DIR)

city_list = list(city_df.select("coord").toPandas()['coord'])
dist = DistanceMetric.get_metric('haversine')
city_tree = BallTree(city_list, leaf_size=20, metric = 'haversine')

del city_list

df_joined = spark.read.parquet(INTER_MAIN_DIR)

pred_udf = F.udf(lambda coord: city_tree.query([coord], return_distance=False).tolist()[0][0])
df_joined = df_joined.withColumn("id", pred_udf(df_joined["coord_df"]))

#df_joined.repartition(300, "id").write.bucketBy(300, "id").sortBy("id").mode("overwrite").format("parquet").saveAsTable("ookla.table2019")
df_joined = df_joined.join(city_df, "id")

df_joined = df_joined.withColumn("country", city_udf(df_joined["tile"]))

df_joined = df_joined.select("avg_d_kbps_21", "avg_u_kbps_21",
        "avg_lat_ms_21", "tests_21", "devices_21",
        "avg_d_kbps_19", "avg_u_kbps_19",
        "avg_lat_ms_19", "tests_19", "devices_19",
        "coord_df", "coord", "city", "country")

df_joined.coalesce(40).write.mode("overwrite").parquet(OUTPUT_DIR)

