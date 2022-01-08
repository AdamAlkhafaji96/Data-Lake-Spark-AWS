# Sparkify Data Lake with Apache Spark on AWS

# Purpose:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. 
This pipeline extracts their data from S3, processes them using Spark, deploy this Spark process on a cluster using AWS, and loads the data back into S3 as a set of dimensional tables. 

# Datasets: 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data

Song dataset is a subset of the Million Song Dataset.
Log datasets have been created by this event simulator.

# Project Structure
    etl.py --- reads data from S3, processes that data using Spark, and writes them back to S3
    dl.cfg --- contains your AWS credentials
    README.md --- provides discussion on your process and decisions
analysis --- [Optional] Provide example queries and results for song play analysis.
    
# Project Requirements
    Spark
    AWS account

# Project Instuctions
1. Create or navigate to an AWS S3 bucket and add the required aws login credentials to the dl.cfg file.
2. Type run etl.py in your terminal to start the ETL process.

