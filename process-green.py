import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
datetimeFormat = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"

green_2 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "green_tripdata_2", transformation_ctx = "green").toDF().createOrReplaceTempView("green_2")
green_3 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "green_tripdata_3", transformation_ctx = "green").toDF().createOrReplaceTempView("green_3")
green_4 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "green_tripdata_4", transformation_ctx = "green").toDF().createOrReplaceTempView("green_4")
nyc_location1 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "nyc_location", transformation_ctx = "green").toDF().createOrReplaceTempView("nyc_location1")
nyc_location2 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "nyc_location", transformation_ctx = "green").toDF().createOrReplaceTempView("nyc_location2")

spark.sql(
"""
select to_timestamp(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')  as pickup_datetime, 
to_timestamp(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')  as dropoff_datetime, 
null as pulocationid,
null as dolocationid,
pickup_longitude,
pickup_latitude,
null as pickup_zone,
null as pickup_borough,
dropoff_longitude,
dropoff_latitude,
null as dropoff_zone,
null as dropoff_borough,
trip_distance
from green_2
union
select to_timestamp(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')  as pickup_datetime, 
to_timestamp(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')  as dropoff_datetime, 
pulocationid,
dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
doloc.longitude as dropoff_longitude,
doloc.latitude as dropoff_latitude,
doloc.zone as dropoff_zone,
doloc.borough as dropoff_borough,
trip_distance
from green_3 g3 
left join nyc_location1 puloc on puloc.locationid = g3.pulocationid
left join nyc_location2 doloc on doloc.locationid = g3.dolocationid
union
select to_timestamp(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')  as pickup_datetime, 
to_timestamp(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')  as dropoff_datetime, 
pulocationid,
dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
doloc.longitude as dropoff_longitude,
doloc.latitude as dropoff_latitude,
doloc.zone as dropoff_zone,
doloc.borough as dropoff_borough,
trip_distance
from green_4 g4 
left join nyc_location1 puloc on puloc.locationid = g4.pulocationid
left join nyc_location2 doloc on doloc.locationid = g4.dolocationid

"""
).write.parquet('s3://tlc.curated/green/')
    
job.commit()


