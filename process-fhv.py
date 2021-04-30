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

fhv_tripdata_1 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "fhv_tripdata_1", transformation_ctx = "fhv").toDF().createOrReplaceTempView("fhv_1")
fhv_tripdata_2 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "fhv_tripdata_2", transformation_ctx = "fhv").toDF().createOrReplaceTempView("fhv_2")
fhv_tripdata_3 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "fhv_tripdata_3", transformation_ctx = "fhv").toDF().createOrReplaceTempView("fhv_3")
fhv_tripdata_4 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "fhv_tripdata_4", transformation_ctx = "fhv").toDF().createOrReplaceTempView("fhv_4")
fhv_tripdata_5 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "fhv_tripdata_5", transformation_ctx = "fhv").toDF().createOrReplaceTempView("fhv_5")
fhvhv_tripdata_1 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "fhvhv_tripdata_1", transformation_ctx = "fhv").toDF().createOrReplaceTempView("fhvhv_1")
nyc_location1 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "nyc_location", transformation_ctx = "green").toDF().createOrReplaceTempView("nyc_location1")
nyc_location2 = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc", table_name = "nyc_location", transformation_ctx = "green").toDF().createOrReplaceTempView("nyc_location2")



spark.sql(
"""
select to_timestamp(pickup_date, 'yyyy-MM-dd HH:mm:ss') as pickup_datetime, 
null as dropoff_datetime, 
f1.locationid as pulocationid,
null as dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
null as dropoff_longitude,
null as dropoff_latitude,
null as dropoff_zone,
null as dropoff_borough
from fhv_1 f1
left join nyc_location1 puloc on puloc.locationid = f1.locationid
union
select to_timestamp(pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as pickup_datetime, 
to_timestamp(dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') as dropoff_datetime, 
pulocationid,
dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
doloc.longitude as dropoff_longitude,
doloc.latitude as dropoff_latitude,
doloc.zone as dropoff_zone,
doloc.borough as dropoff_borough
from fhv_2 f2
left join nyc_location1 puloc on puloc.locationid = f2.pulocationid
left join nyc_location2 doloc on doloc.locationid = f2.dolocationid
union
select to_timestamp(pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as pickup_datetime, 
to_timestamp(dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') as dropoff_datetime, 
pulocationid,
dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
doloc.longitude as dropoff_longitude,
doloc.latitude as dropoff_latitude,
doloc.zone as dropoff_zone,
doloc.borough as dropoff_borough
from fhv_3 f3
left join nyc_location1 puloc on puloc.locationid = f3.pulocationid
left join nyc_location2 doloc on doloc.locationid = f3.dolocationid
union
select to_timestamp(pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as pickup_datetime, 
to_timestamp(dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') as dropoff_datetime, 
pulocationid,
dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
doloc.longitude as dropoff_longitude,
doloc.latitude as dropoff_latitude,
doloc.zone as dropoff_zone,
doloc.borough as dropoff_borough
from fhv_4 f4
left join nyc_location1 puloc on puloc.locationid = f4.pulocationid
left join nyc_location2 doloc on doloc.locationid = f4.dolocationid
union
select to_timestamp(pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as pickup_datetime, 
to_timestamp(dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') as dropoff_datetime, 
pulocationid,
dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
doloc.longitude as dropoff_longitude,
doloc.latitude as dropoff_latitude,
doloc.zone as dropoff_zone,
doloc.borough as dropoff_borough
from fhv_5 f5
left join nyc_location1 puloc on puloc.locationid = f5.pulocationid
left join nyc_location2 doloc on doloc.locationid = f5.dolocationid
union
select to_timestamp(pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as pickup_datetime, 
to_timestamp(dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') as dropoff_datetime, 
pulocationid,
dolocationid,
puloc.longitude as pickup_longitude,
puloc.latitude as pickup_latitude,
puloc.zone as pickup_zone,
puloc.borough as pickup_borough,
doloc.longitude as dropoff_longitude,
doloc.latitude as dropoff_latitude,
doloc.zone as dropoff_zone,
doloc.borough as dropoff_borough
from fhvhv_1 fh1
left join nyc_location1 puloc on puloc.locationid = fh1.pulocationid
left join nyc_location2 doloc on doloc.locationid = fh1.dolocationid

"""
).write.parquet('s3://tlc.curated/fhv/')
    
job.commit()


