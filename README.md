# NYC TLC Analytics
# Objective
To demonstrate AWS capability to build robust analytics pipeline on public traffic data

# Data Source
New York City Taxi and Limousine Commission (TLC) Trip Record Data: https://registry.opendata.aws/nyc-tlc-trip-records-pds/

# Scope
1. Retrieve data from raw format 
2. Perform data analysis (feature engineering) for data ETL 
3. Illustrate request and demands trends, especially for the gap trends of taxi for data analyst for insight discovery 
4. Provide graphic illustration for operators to understand the changes of demands gap in different area (location ID). If possible, you could make a short 10-min prediction 
5. Simulate a real-time display of GUI on 10-min based data set for operators 
6. Let drivers to see the demands level in 10 min drive distance. If possible, display the past, current and future in graphics 
7. Setup a portal for different users to login to watch different GUI 

# Assumption
1. Only start to process data from 2015, though the full set of data would be saved to raw bucket 
2. The longitude and latitude could be estimated as the center point of bound box for the zone’s shape
3. Some analysis will drop the outline data: pickup date <2015 or pickup date > 2020; travel time <= 60 sec; travel speed > 50 miles/hour


# Architecture 
The architecture uses AWS serverless components which has below benefits:
1. Build-in scalability and elasticity: could automatically scale up and down according to demand
2. Built-in fault tolerance and availability
3. Improve operational efficiency: easily handle back up, encryption, restore; minimum administrative workload e.g. security patches, platform upgrade, no server management  
4. Reduce cost: no idle resources, pay as you go
 
1. Analytics pipeline
a. Copy all raw files from NYC TLC public bucket to nyc-raw bucket, copy files into separate prefix according to schema
b. Use Glue Crawler to crawl yellow, green, fhv, fhvhv data (since 2015), generate tables of data catalog for raw data.
c. Use Glue jobs to process data, and store the results to nyc-curated bucket
d. Use Glue Crawler to crawl the curated data and generate tables of data catalog for processed data
e. Use Athena to query the data
f. Use Quicksight to analyse and visualize the results
![image](https://user-images.githubusercontent.com/15627541/119111342-8e75bf80-ba55-11eb-944a-2b6976e20b8e.png)

2. User portal
a. User (driver or analytics user) could visit the portal through API gateway endpoint
b. API gateway will route request to Lambda function which will check if the user has logged in with a valid token or redirect to Cognito for user login. 
c. User login with Cognito and Cognito triggers call back to Lambda function with a valid token
d. Lambda function retrieve Quicksight dashboard info including embedded url and return to client
e. User directly interact with Quicksight dashboard with embedded url
f. Different groups of users (driver or analytics user) will have different access
![image](https://user-images.githubusercontent.com/15627541/119111373-96cdfa80-ba55-11eb-8727-68c3bebc9856.png)
 
# User portal
https://duot950xbf.execute-api.us-east-1.amazonaws.com/v1/tlc-dashboard
Login users:
Analytics user: kevin/Qwert!2345
Driver: driverA/Qwert!2345

# Source Code
https://github.com/kevin4work/nyc-tlc-pipeline

# Run on Glue:
copy-raw.py: copy raw files from nyc-tlc bucket
process-fhv.py: process fhv data and re-format to parquet
process-green.py: process green taxi data and re-format to parquet
process-green.py: process yellow taxi data and re-format to parquet
merge-all.py: merge all processed data and repartition to year/month/day

# Run on notebook:
get-location.py: get zone longitude and latitude estimation by shape file

# Run on lambda:
Refer to: https://learnquicksight.workshop.aws/en/dashboard-embedding/3.embedding-framework/5.web-content-lambda.html
