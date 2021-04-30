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

yellow_tripdata_1 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "yellow_tripdata_1", transformation_ctx = "yellow").toDF().createOrReplaceTempView("yellow_1")
yellow_tripdata_2 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "yellow_tripdata_2", transformation_ctx = "yellow").toDF().createOrReplaceTempView("yellow_2")
yellow_tripdata_3 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "yellow_tripdata_3", transformation_ctx = "yellow").toDF().createOrReplaceTempView("yellow_3")
yellow_tripdata_4 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "yellow_tripdata_4", transformation_ctx = "yellow").toDF().createOrReplaceTempView("yellow_4")
yellow_tripdata_5 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "yellow_tripdata_5", transformation_ctx = "yellow").toDF().createOrReplaceTempView("yellow_5")
yellow_tripdata_6 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "yellow_tripdata_6", transformation_ctx = "yellow").toDF().createOrReplaceTempView("yellow_6")
yellow_tripdata_7 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "yellow_tripdata_7", transformation_ctx = "yellow").toDF().createOrReplaceTempView("yellow_7")
nyc_location1 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "nyc_location", transformation_ctx = "yellow").toDF().createOrReplaceTempView("nyc_location1")
nyc_location2 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "nyc_location", transformation_ctx = "yellow").toDF().createOrReplaceTempView("nyc_location2")
    

spark.sql(
"""
select to_timestamp(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')  as pickup_datetime, 
to_timestamp(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')  as dropoff_datetime, 
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
from yellow_4
union
select to_timestamp(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')  as pickup_datetime, 
to_timestamp(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')  as dropoff_datetime, 
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
from yellow_5
union
select to_timestamp(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')  as pickup_datetime, 
to_timestamp(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')  as dropoff_datetime, 
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
from yellow_6 y6 
left join nyc_location1 puloc on puloc.locationid = y6.pulocationid
left join nyc_location2 doloc on doloc.locationid = y6.dolocationid
union
select to_timestamp(tpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss')  as pickup_datetime, 
to_timestamp(tpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss')  as dropoff_datetime, 
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
from yellow_7 y7 
left join nyc_location1 puloc on puloc.locationid = y7.pulocationid
left join nyc_location2 doloc on doloc.locationid = y7.dolocationid

"""
).write.parquet('s3://tlc.curated/yellow/')
    
job.commit()


