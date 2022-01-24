from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import shapely.wkt
import json
import urllib.request
import geopandas as gpd

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

GEOJSON_PATH = 'https://datahub.io/core/geo-countries/r/countries.geojson'

d_type = 'fixed'
year = 2021
quart = 'q4'

DATA_PATH = '/user/s2665220/project/data/input/{ear}-{quarter}/*_{dat_type}_*.parquet'.format(ear = year, quarter=quart, dat_type = d_type)

OUTPUT_DIR = '/user/s2665220/project/data/input/country_data/{ear}/{quarter}_{dat_type}'.format(ear = year, quarter=quart, dat_type = d_type)

df = spark.read.parquet(DATA_PATH)

with urllib.request.urlopen(GEOJSON_PATH) as url:
    gj = json.loads(url.read().decode())

geo_country = gpd.GeoDataFrame.from_features(gj["features"]).append({'ADMIN': 'N/A'}, ignore_index=True)

def get_country(p):
    ind = geo_country.geometry.sindex.query(shapely.wkt.loads(p), "within")
    if len(ind) == 0:
        return 'N/A'
    else:
        return geo_country['ADMIN'][ind[0]]

country_udf = F.udf(lambda p: get_country(p), T.StringType())

#p = p.apply(wkt.loads)

#@F.pandas_udf(T.ObjectType(), F.PandasUDFType.SCALAR)
#def get_country(p):
#    ind = geo_country.geometry.sindex.query_bulk(p.apply(wkt.loads), "within")
#    start, end = ind[0][0], ind[0][-1]
#    temp_df = pd.DataFrame('N/A', index=range(start, end+1), columns=['location'], dtype='string')
#    temp_series = geo_country.ADMIN.iloc[ind[1]]
#    temp_series.index = ind[0]
#    temp_df.iloc[ind[0]] = temp_series
#    return temp_df.location
    #if len(ind) == 0:
    #    return 'N/A'
    #else:
    #    return geo_country['ADMIN'][ind[0]]

df = df.withColumn("country", country_udf(df["tile"]))

df = df.drop('quadkey', 'tile')

df = df.groupBy("country").agg(
        F.avg("avg_d_kbps").alias("avg_d_kbps"),
        F.avg("avg_u_kbps").alias("avg_u_kbps"),
        F.avg("avg_lat_ms").alias("avg_lat_ms"),
        F.sum("tests").alias("tests"), F.sum("devices").alias("devices"))

df.coalesce(1).write.mode("overwrite").csv(OUTPUT_DIR)

