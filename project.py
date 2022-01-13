import logging
import os
import re
from random import randint
from time import sleep

import geopandas as gpd
import pandas as pd
from geopy.adapters import AioHTTPAdapter
from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from geopy.geocoders import Nominatim
from shapely.geometry import Polygon
from tqdm import tqdm


def transform_polygon_string_to_tuple(polygon_string):
    subst = ""
    polygon_regex = r"(POLYGON\(\()(-?\d*\.\d*\s-?\d*\.\d*)"
    result = re.search(polygon_regex, polygon_string)
    polygon_string = result.group(2)
    return tuple(polygon_string.split(' '))


def geopy_client_geo_reverse(geolocator, coordinate, file_name):
    location = geolocator.reverse(coordinate, timeout = 3, language="en")
    country = location.raw.get('address').get('country')
    print("Coordinates " + coordinate , " Country: " + country , " file name: " + file_name)
    return country

    
def convert_polygon_to_country(poly_items, file_name):
    logging.basicConfig(filename="logger.log",format='%(asctime)s %(message)s \n',filemode='w')
    logger = logging.getLogger()
    return_list= [] 
    index = 0
    print(str(len(poly_items)) + " rows have been given initially to the script ")
    for poly_item in tqdm(poly_items):
        longitude,latitude = transform_polygon_string_to_tuple(poly_item)[:2]
        my_user_agent = 'UTwente_Managing_Big_Data_project_{}'.format(randint(10000,99999))
        geolocator = Nominatim(user_agent = my_user_agent)
        coordinate = str(latitude) +","+ str(longitude)
        try:
            country = geopy_client_geo_reverse(geolocator, coordinate, file_name)
            return_list.append(country)
            index = index +1
        except GeocoderTimedOut:
            logger.info('TIMED OUT: GeocoderTimedOut: Retrying...')
            sleep(randint(1*100,5*100)/100)
            country = geopy_client_geo_reverse(geolocator, coordinate, file_name)
            return_list.append(country)
        except GeocoderServiceError as e:
            logger.error('CONNECTION REFUSED: GeocoderServiceError encountered.')
            logger.error(e)
            return None
        except Exception as e:
            logger.error('ERROR: Terminating due to exception {}'.format(e))
            logger.error(e)
            return None
    print(str(index) + " have been processed ")
    print(str(len(poly_items)) + " have been given initially to the script ")
    return return_list

for subdir, dirs, files in os.walk('dataset'):
    for file in files:
        if file.endswith('parquet'):
            file_name = os.path.join(subdir,file)
            print("Reading now from " + file_name )
            shape_file = pd.read_parquet(file_name, engine='pyarrow')
           
            new_file_name = 'countries_' + file.split('.')[0] +'.parquet'
            new_file_path = os.path.join(subdir,new_file_name)
            
            print("Create new parquet files ..............")
            
            parquet_file = pd.DataFrame(columns=['quadkey','avg_d_kbps','avg_u_kbps','avg_lat_ms','tests','devices', 'tile'])
            parquet_file.quadkey = shape_file.quadkey
            parquet_file.avg_d_kbps = shape_file.avg_d_kbps
            parquet_file.avg_u_kbps = shape_file.avg_u_kbps
            parquet_file.avg_lat_ms = shape_file.avg_lat_ms
            parquet_file.tests = shape_file.tests
            parquet_file.devices = shape_file.devices

            print("Converting polygons to countries ..............")
            parquet_file.tile = convert_polygon_to_country(shape_file.tile,file_name)
            # parquet_file = pd.read_parquet('dataset/2019-q1/countries_gps_fixed_tiles.parquet', engine='pyarrow')
            print("Writing now to " + new_file_path)
            parquet_file.to_parquet(new_file_path)
            print("Finished ...................................................")
            
