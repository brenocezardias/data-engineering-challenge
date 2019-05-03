# Data Engineering Challenge
This repository contains a project for Data Engineering Challenge

## How to run
  You will need a machine with linux (preferably ubuntu) installed:
  > This code has been tested in ubuntu 18.04.2
  
In case that you don't have spark installed on yor machine, 
see the [config](https://github.com/brenocezardias/data-engineering-challenge/tree/master/config) topic to install:
  > This code has been tested in spark 2.4.2 and Python 2.7.15rc1

**Now that you have all set ! Let's create the table**

See the [create_table](https://github.com/brenocezardias/data-engineering-challenge/tree/master/config) on config

or just copy bellow

```sql
  CREATE TABLE campaign_analytics (
	device_id     		VARCHAR(50),
	lead_id	      		INT,
	registered_at 		TIMESTAMP,
	credit_decision 	VARCHAR(20),
	credit_decision_at      TIMESTAMP,
	signed_at 	        TIMESTAMP,
	revenue 	        float8,
	ad_creative_id 		VARCHAR(50),
	campaign_id 		INT,
	source 			VARCHAR(30),
	ad_creative_name 	VARCHAR(100),
	campaign_name 		VARCHAR(100),
	clicks 			float8,
	cost 			float8,
	impressions 		BIGINT
);
```
## Data

 * The files used are in [Data](https://github.com/brenocezardias/data-engineering-challenge/tree/master/Data) directory 
 except for the pageview, because they exceed the allowed size

## Running the script

To run the script, you will need to set some things:
  1. Allocate files in an directory and set the path in `dir_files` on `1. EXTRACT DATA` in the code.
     The script reads, and writes files in an directory, i set the path `/home/`
     
  2. Set the configs of your database on `3.LOAD DATA` in code, look below as it is
  
     `jdbc_url = "jdbc:postgresql://127.0.0.1:5432/postgres"`
     
     `properties = {"user": "postgres","password": "bcdias123", "driver": "org.postgresql.Driver"}`
  
  3. Allocate the PostgreSQL Driver in an directory, for Spark,
  
     In case that you don't have, see the [lib](https://github.com/brenocezardias/data-engineering-challenge/tree/master/lib) folder
     
  4. Use the command bellow to run the script
  
     The script are in [Etl_Digital_Media](https://github.com/brenocezardias/data-engineering-challenge/tree/master/src)
  
     `/home/breno/spark/spark-2.4.2-bin-hadoop2.7/bin/spark-submit --driver-class-path /vagrant/lib/postgresql-42.1.4.jar /home/arquivos/Etl_Digital_Media.py`
     
     remember to change the paths to the corresponding ones in your machine **(spark path) > (Driver path) > (Script path)**
     
  ## Queryes
  
**QUERY 1 (What was the most expensive campaign?)**

```sql
select campaign_id,
       campaign_name,
       source,
       cost
from campaign_analytics
where cost in (select max(cost) 
               from campaign_analytics limit 1) 
limit 1;
```

**QUERY 2 (What was the most profitable campaign?)**

```sql
select campaign_id,
	   count(campaign_id) as quantidade
from campaign_analytics
where revenue is not null
group by campaign_id
having count(campaign_id) = (select max(campaign_id.count)
		             from (select distinct count(campaign_id) as count
			     from campaign_analytics
		             where revenue is not null
			     group by (campaign_id)) as campaign_id);
```
**QUERY 3 (Which ad creative is the most effective in terms of clicks?)**

```sql
select ad_creative_id,
       ad_creative_name,
       source,
       clicks
from campaign_analytics
where clicks in (select max(clicks) 
		 from campaign_analytics limit 1) 
limit 1;
```
**QUERY 4 (Which ad creative is the most effective in terms of generating leads?)**

```sql
select ad_creative_id,
       count(ad_creative_id) as leads_number
from campaign_analytics
where ad_creative_id is not null
group by ad_creative_id
having count(ad_creative_id) = (select max(ad_creative_id.count)
				                        from (select distinct count(ad_creative_id) as count
				                              from campaign_analytics
				                              where ad_creative_id is not null
				                              group by (ad_creative_id)) as ad_creative_id);
 ```
 
 ## Extra Questions
 
 **What would you suggest to process new incoming files several times a day?**
 
 Answer: I suggest creating a data pipeline, using the Apache frameworks, for storing and processing that data (Hadoop HDFS and Spark)
 
 **What would you suggest to process new incoming data in near real time?**
 
 Answer: For real-time data processing I suggest using Apache Kafka, a unified, high-capacity, low latency platform for real-time data processing
 
 **What would you suggest to process data that is much bigger?**
 
 Answer: I suggest the use of a distributed computing system, such as Spark, which has the intelligence to process data in different clusters, splitting into packets, so if one cluster fails, others still work
 
 **What would you suggest to process data much faster?**
 
 Answer: I suggest the use of Spark, because one of the reasons for its creation, is to meet the lack of performance of Hadoop Map Reduce, Spark saves the data in memory, while, Hadoop keeps in disk, this makes the Spark up to 100x more fast.

thank you !
