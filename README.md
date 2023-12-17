# ASTEROID VAULT
### BY Sameer 

This is a project to study asteroids, their whereabouts and studying them with advanced modeling and data science.
The project proposes value being a complete production ready system and advanced data analytics used.
I have used the stack I prefer the most, while equilavent can be accomplished by myriads of other arrangements.

## CONTEXT AND INITIAL PROBLEM STATEMENT
NASA has this set of amazing APIs, espeaciall `Asteroids - NeoWs` set of endpoints.
While the data from the portal is great and all, NASA only provides Hourly Limit: 1,000 requests per hour as of now, this could be a problem BUT
thats not the major issue.

The API can only give data of a period upto 7 days, while thats decent, its nowhere near to be explored to mine and gather information of the bulk.

## SOLUTION 
This project has two major part (complementing eachother) : 
    - setting up Infrastructures
    - exploring data analytics

### INFRASTRUCTURE SOLUTION
The pipeline begins with a orchestration that streams data from NASA web portal and dumps it into a warehouse. The warehouse can be realized with a technology of your choice, i will be using cassandra and AWS s3, while this can be implemented with anyother datalake/warehouse systems like HDFS etc.
The elephant in the room has to be adressed now, since the new data is being streamed and all, we would want to get an insight from previous data if we could look at it altogether instead of 7 days time frame of the API. So, theres another catchup orchestration that based on the ENV variables catches up to previous data records. It achieves this in chronological reverse order that is this week, last week, last last week and so on. While the streaming orchestration is up and running, the catchup orchestration can be run in background and can be disabled once reached threshold of previous data, as value is proportional to the recency of data.
#### ETL ORCHESTRATION
    - catch_up_dag.py
    - load_warehouse_dag.py

#### WAREHOUSE ORCHESTRATION
While CQL can be used if cassandra is the quote/unquote warehouse for the system.
But if the data is being loaded to AWS S3, AWS Redshift tables can be created on top of it using `glueContext.write_dynamic_frame.from_catalog([..])`
BASICALLY, once a complete, contraint, well governed and low latency data warehouse has been resurrected, the next part would be data science and analytics.

# DATA SCIENCE AND ANALYTICS
# NASA_ASTEROIDS_VAULT
