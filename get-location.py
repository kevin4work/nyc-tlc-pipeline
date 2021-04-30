%conda install PyShp
%conda install pyproj
import pandas as pd
import zipfile
import math
import shapefile
import sys
import pyproj
​
nyc=pyproj.CRS("+proj=lcc +lat_1=40.66666666666666 +lat_2=41.03333333333333 +lat_0=40.16666666666666 +lon_0=-74 +x_0=300000 +y_0=0 +datum=NAD83 +units=us-ft +no_defs")
wgs84=pyproj.CRS("EPSG:4326")
​
with zipfile.ZipFile("taxi_zones.zip","r") as zip_ref:
    zip_ref.extractall("./shape")
def get_lat_lon(sf):
    content = []
    for sr in sf.shapeRecords():
        shape = sr.shape
        rec = sr.record
        loc_id = rec[shp_dic['LocationID']]
        
        x1 = (shape.bbox[0]+shape.bbox[2])/2
        y1 = (shape.bbox[1]+shape.bbox[3])/2
        x2,y2 = pyproj.transform(nyc,wgs84,x1,y1)
        content.append((loc_id, x2, y2))
    return pd.DataFrame(content, columns=["LocationID", "latitude", "longitude"])
sf = shapefile.Reader("shape/taxi_zones.shp")
fields_name = [field[0] for field in sf.fields[1:]]
shp_dic = dict(zip(fields_name, list(range(len(fields_name)))))
attributes = sf.records()
shp_attr = [dict(zip(fields_name, attr)) for attr in attributes]
​
df_loc = pd.DataFrame(shp_attr).join(get_lat_lon(sf).set_index("LocationID"), on="LocationID")
df_loc.head()
df_loc.to_csv("nyc-location.csv")
​
