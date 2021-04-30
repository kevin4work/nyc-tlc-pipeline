import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import boto3
from botocore.exceptions import ClientError

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
client = boto3.client('s3')
sourceBucket = 'nyc-tlc'
targetBucket = 'tlc.raw'
tlcPath = 's3://'+sourceBucket+'/'
months = range(1,13)

def keyExisting(client, bucket, key):
    try:
        obj = client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response['Error']['Code'] != '404':
            raise
        else:
            return False
        
def copyRaw(prefix, years, months, prevHeader, schemaCount):
    for year in years:
        strYear = str(year)
        for month in months:
            strMonth = str(month)
            if len(strMonth) == 1:
                strMonth = "0"+strMonth
            key = prefix + strYear + '-' + strMonth + '.csv'
            if keyExisting(client, sourceBucket, key):
                filename = tlcPath + key
                currHeader = sc.textFile(filename).first().strip()
                if (prevHeader == '' or prevHeader != currHeader):
                    print(filename)
                    print(currHeader)
                    prevHeader = currHeader
                    schemaCount+=1
                copy_source = {
                    'Bucket': sourceBucket,
                    'Key': key
                 }
                targetKey =  prefix + str(schemaCount) + '/' + strYear + '-' + strMonth + '.csv'
                client.copy(copy_source, targetBucket, targetKey)

## fhv data
years = range(2015, 2021)
prefix = 'trip data/fhv_tripdata_'
prevHeader = ''
schemaCount = 0
copyRaw(prefix, years, months, prevHeader, schemaCount)

## fhvhv data
years = range(2019, 2021)
prefix = 'trip data/fhvhv_tripdata_'
prevHeader = ''
schemaCount = 0
copyRaw(prefix, years, months, prevHeader, schemaCount)

## green data
years = range(2013, 2021)
prefix = 'trip data/green_tripdata_'
prevHeader = ''
schemaCount = 0
copyRaw(prefix, years, months, prevHeader, schemaCount)

## yellow data
years = range(2009, 2021)
prefix = 'trip data/yellow_tripdata_'
prevHeader = ''
schemaCount = 0
copyRaw(prefix, years, months, prevHeader, schemaCount)

job.commit()