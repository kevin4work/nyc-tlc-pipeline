import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

fhv = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc-curated", table_name = "curated_fhv", transformation_ctx = "merge").toDF()
green = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc-curated", table_name = "curated_green", transformation_ctx = "merge").toDF()
yellow = glueContext.create_dynamic_frame.from_catalog(database = "nyc-tlc-curated", table_name = "curated_yellow", transformation_ctx = "merge").toDF()

fhv = fhv.select(lit("FHV").alias("Category"), "pickup_datetime", "pickup_zone").withColumn("year", date_format(col("pickup_datetime"), "yyyy")) \
           .withColumn("month", date_format(col("pickup_datetime"), "MM")) \
           .withColumn("day", date_format(col("pickup_datetime"), "dd")) 
green = green.select(lit("Green").alias("Category"), "pickup_datetime", "pickup_zone").withColumn("year", date_format(col("pickup_datetime"), "yyyy")) \
           .withColumn("month", date_format(col("pickup_datetime"), "MM")) \
           .withColumn("day", date_format(col("pickup_datetime"), "dd")) 
yellow = yellow.select(lit("Yellow").alias("Category"), "pickup_datetime", "pickup_zone").withColumn("year", date_format(col("pickup_datetime"), "yyyy")) \
           .withColumn("month", date_format(col("pickup_datetime"), "MM")) \
           .withColumn("day", date_format(col("pickup_datetime"), "dd")) 

merged = fhv.union(green).union(yellow)
##merged.filter((merged.year >= 2015) & (merged.year < 2021)).write.partitionBy("year", "month", "day").parquet('s3://tlc.curated/merged/')
merged.filter((merged.year >= 2015) & (merged.year < 2021)).repartition("year", "month", "day").write.partitionBy("year", "month", "day").mode("append").parquet('s3://tlc.curated/merged-all/')

