"""
Run as follows:
time spark-submit --conf  "spark.pyspark.python=/usr/bin/python3.6" --conf "spark.pyspark.driver.python=/usr/bin/python3.6" get_countries_sample.py 2> /dev/null
"""

from pyspark.sql import SparkSession
from shapely import wkt  
from shapely.geometry import shape
import json

from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

## Use pyspark df read json method to read from hdfs
# geojson_file = '/user/s2801620/project/data/input/countries.geojson'
geojson_file = '/home/s2801620/proj/countries.geojson'

with open(geojson_file) as f:
    gj =json.load(f)

country_polys = {}
for feature in gj['features']:
    country = feature['properties']['ADMIN']
    geometry = feature['geometry']
    country_polys[country] = shape(geometry)

DATA_PATH_SAMPLE = '/user/s2665220/project/data/input/sample.csv'
dfs = spark.read.option('header', True).csv(DATA_PATH_SAMPLE)

def get_country(polygon):
    for count in country_polys:
        if polygon.within(country_polys[count]):
            return count
    return 'NA'

def row_func(row):
    row_dict = row.asDict()
    row_dict['country'] = get_country(wkt.loads(row_dict['tile']))
    newrow = Row(**row_dict)
    return newrow

rdds = dfs.rdd
rdds_n = rdds.map(lambda row: row_func(row))
dfs_n = spark.createDataFrame(rdds_n)
dfs_n.write.option("header",True).csv('/user/s2801620/project/sampleout')