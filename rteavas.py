#By Andrew Paladino, April 10, 2022 
#Apache Kafka download is required, with both zookeeper + kafka running for the cosumer & producer to print messages

#####################
### DEPENDENCIES ####
#####################

from pyspark.sql import SparkSession
import requests
import pandas as pd
import geopandas as geo
import numpy as np
from tqdm import tqdm
from functools import reduce
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from shapely.geometry import Point, Polygon, MultiPolygon, MultiPoint
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ArrayType, StringType
import osmnx as ox

# Initialize SparkSession
spark = SparkSession.builder.appName("rteavas").getOrCreate()

# Load necessary packages
requests.packages.urllib3.disable_warnings()
tqdm.pandas()

# Load state_abbv DataFrame
state_ab = pd.read_csv('data_dir/state_abbv.csv')

#####################
##### PART ONE ######
#####################

# Define function to retrieve NWS alerts for a state
def get_alerts(state):
    url = f'https://api.weather.gov/alerts/active?area={state}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
    else:
        print(f'Error: {response.status_code}')
        return pd.DataFrame()

    alerts = []
    for alert in data['features']:
        properties = alert['properties']
        if properties['severity'] == 'Severe':
            alert_dict = {
                'state': state,
                'area': properties['areaDesc'],
                'event': properties['event'],
                'sent': properties['sent'],
                'onset': properties['onset'],
                'ends': properties['ends'],
                'geocode': properties['geocode']['SAME'],
                'severity': properties['severity'],
                'certainty': properties['certainty'],
                'urgency': properties['urgency'],
                'headline': properties['headline'],
                'descr': properties['description']
            }
            alerts.append(alert_dict)

    if len(alerts) == 0:
        return pd.DataFrame()
    
    report = pd.DataFrame(alerts)
    report['sent'] = pd.to_datetime(report['sent'], utc=True)
    report['onset'] = pd.to_datetime(report['onset'], utc=True)
    report['ends'] = pd.to_datetime(report['ends'], utc=True)
    report.to_csv('report.csv')
    return report

# Create Spark DataFrame with state_abbv
states = spark.createDataFrame(state_ab)

# Use map function to retrieve alerts for each state in parallel
alerts = states.rdd.map(lambda row: row['state_abbv']).map(get_alerts)

# Use reduce function to concatenate all of the Pandas DataFrames into one final DataFrame
final_report = reduce(lambda df1, df2: pd.concat([df1, df2]), alerts.collect())
spark_report = spark.createDataFrame(final_report)

#####################
##### PART TWO ######
#####################

counties = geo.read_file('data_dir/c_13se22/c_13se22.shp')

# Extract list of affected geocodes from spark_report DataFrame
affected = []
for row in spark_report.select('geocode').collect():
    for code in row['geocode']:
        code = code.lstrip('0')
        affected.append(code)
affected_nd = list(set(affected))
affected_nd.sort(reverse=False)

# Create GeoDataFrame of affected counties
affected_counties = geo.GeoDataFrame()
for f in affected_nd:
    counties['FIPS'] = counties['FIPS'].str.lstrip('0')
    county = counties[(counties['FIPS'] == f)]
    county = county.to_crs(4326)
    affected_counties = pd.concat([affected_counties, county])
affected_counties.to_csv('affected_counties.csv')
    
# Merge affected counties with spark_report DataFrame
spark_report = spark_report.withColumn('geocode', explode('geocode'))
spark_report = spark_report.dropDuplicates(['geocode', 'event', 'area'])
#spark_report = spark_report.withColumn('geocode', ltrim('geocode', 0))
spark_report = spark_report.withColumn('geocode', F.regexp_replace('geocode', r'^0+', ''))
spark_report.show()
spark_report.printSchema()
merged_affected = spark_report.toPandas().merge(affected_counties, left_on='geocode', right_on='FIPS', how='left')
geo_merge = geo.GeoDataFrame(merged_affected, geometry='geometry')
geo_merge.to_csv('geo_merge.csv')
geo_merge = geo_merge.to_crs(4326)


#####################
##### PART THREE ####
#####################

def extract_utilities(geom, tags):
    osm_pull = ox.geometries.geometries_from_polygon(geom, tags)
    osm_pull.reset_index(inplace=True)
    return osm_pull

# Define the tags to use for the OSM query
tags = {'power': 'plant'}

# Parallelize the extraction of utilities
affected_utilities_rdd = (spark.sparkContext
                         .parallelize(geo_merge.geometry.values)
                         .map(lambda geom: extract_utilities(geom, tags)))

# Combine the extracted utilities into a single DataFrame
affected_utilities_df = pd.concat(affected_utilities_rdd.collect())
affected_utilities_df.to_csv('affected_utilities.csv', index=False)

#####################################################
### SOME CLEANING + MERGING ~ NO SPARK INVOLVED #####
#####################################################

affected_utilities = affected_utilities_df[['geometry', 'name', 'operator', 'plant:source', 'plant:method',
                              'plant:output:electricity',  'source', 'osmid', 'element_type', 'power']]

### perform a spatial merge on the returned osm_boundaries and the merged file, delete dupes

prod = geo.GeoDataFrame(affected_utilities, geometry='geometry')
merged = geo.sjoin(prod, geo_merge, how='inner', op='intersects')
final = merged.drop_duplicates(subset = 'osmid') 
final['centroid'] = final['geometry'].centroid
final['descr'] = final['descr'].str.wrap(500)
final = final.fillna('NA')

#####################
##### PART FOUR ##### 
#####################

import geopandas as geo
from shapely.geometry import Polygon

current = final[(final.urgency == 'Immediate')]

def extract_forecast(x, y):
    data_list = []
    url = f'https://api.weather.gov/points/{y},{x}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        forecast = data['properties']['forecast']
    else:
        print(f'Error: {response.status_code}')
        return []

    url_forecast = forecast
    response = requests.get(url_forecast)
    if response.status_code == 200:
        data_forecast = response.json()
        detailed_forecast = data_forecast['properties']['periods'][0]['detailedForecast']
        short_forecast = data_forecast['properties']['periods'][0]['shortForecast']
        temperature = data_forecast['properties']['periods'][0]['temperature']
        wind_speed = data_forecast['properties']['periods'][0]['windSpeed'][:-4]
        per_precip = data_forecast['properties']['periods'][0]['probabilityOfPrecipitation']['value']
        elevation = data_forecast['properties']['elevation']['value']
        geojson = data_forecast['geometry']['coordinates']
        polygon = Polygon(geojson[0])
        data_list.append([detailed_forecast, short_forecast, temperature, wind_speed, per_precip, elevation, polygon])
    return data_list

centroids_rdd = (spark.sparkContext
                 .parallelize(zip(current['centroid'].x, current['centroid'].y)))
forecast_rdd = centroids_rdd.flatMap(lambda xy: extract_forecast(xy[0], xy[1]))

# Convert the resulting RDD back to a pandas DataFrame
current_df = pd.DataFrame(forecast_rdd.collect(), 
                          columns=['detailed_forecast', 'short_forecast', 'temperature', 
                                   'wind_speed', 'chance_of_precipitation', 'elevation', 'polygon'])

# Convert the DataFrame to a GeoDataFrame
current_gdf = geo.GeoDataFrame(current_df, geometry='polygon', crs='EPSG:4326')

# Save the GeoDataFrame to a shapefile
current_gdf.to_csv('current_df.csv')

########################################################
##### SOME CLEANING + MERGING ~ NO SPARK INVOLVED ###### 
########################################################

current.drop(columns = 'index_right', inplace = True)
join_current = current.sjoin_nearest(current_gdf, how='inner')
join_current.to_csv('join_current.csv')

############################
##### PART FIVE: KAFKA ##### 
############################

from kafka import KafkaProducer
import json

# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Broadcast messages in description column of join_current dataframe
for index, row in join_current.iterrows():
    # Extract the name of the asset and its detailed forecast
    asset_name = row['name']
    detailed_forecast = row['detailed_forecast']
    
    # Convert the message to a JSON string
    message = json.dumps({'asset_name': asset_name, 'detailed_forecast': detailed_forecast})
    
    # Send the message to the Kafka topic
    producer.send('R-TEAVAS', message.encode('utf-8'))

# Close the Kafka producer
producer.close()

from kafka import KafkaConsumer
import json

# Set up Kafka consumer
consumer = KafkaConsumer('R-TEAVAS', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', enable_auto_commit=True)

# Consume messages from the Kafka topic
for message in consumer:
    # Decode the message
    msg = message.value.decode('utf-8')
    
    # Load the JSON string
    data = json.loads(msg)
    
    # Print the asset name and its detailed forecast
    print(data['asset_name'], data['detailed_forecast'])
    
consumer.close()

############################
############ END ###########
############################
