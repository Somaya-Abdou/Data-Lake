# Data-Lake
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 
This will allow their analytics team to continue finding insights in what songs their users are listening to.
# Schema design :
A fact table and 4 dimensional tables were created using data in udacity bucket . 
these tables were partitioned and saved as parquet data on a different s3 bucket . 
Using Spark and ELT pipeline, data were stored intermediately in spark servers instead of saving them in a bucket before quering 
that made the performance much easier and quicker .
