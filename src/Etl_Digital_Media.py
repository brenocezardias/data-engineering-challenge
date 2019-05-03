# START

#Importing libs to operate with Spark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter

# Build SparkSession
spark = SparkSession.builder.master("local[*]").appName("ETL Digital Media").getOrCreate()

# Set Logging Level to WARN
spark.sparkContext.setLogLevel("WARN")

# 1. EXTRACT DATA

# Gives the directory a name to read and write files
dir_files = '/home/arquivos/'

#Open the files

print(' STEP 1: Loading the files...')

file1 = open(dir_files + 'pageview.txt','r').readlines()

file2 = open(dir_files + 'google_parse.csv','w')

file3 = open(dir_files + 'facebook_parse.csv','w')

#Creating a CSV file for Google campaigns, from the log file
for line in file1:
     if line.find('ad_creative_id') != -1:
             line  = line.replace('&',' | ').replace('=',':').replace('ad_creative_id','')
             line  = line.replace(':','')
             line2 = line
             line  = line.replace('http//google.com.br','google').replace('campaign_id','').replace('device_id','').replace('referer','').replace(' | ','|').replace('| ','|').split('?')[1]
             line2 = line2.split('[')[1].split(']')[0].split(' ')[0] + '|'
             line2 = line2 + line
             file2.write(str(line2))

#Creating a CSV file for Facebook campaigns, from the log file
for line in file1:
     if line.find('?campaign_id') != -1:
	     line  = line.replace('=',':').replace('campaing_id','')
	     line2 = line
             line  = line.replace(':','').replace('http//www.facebook.com','facebook').replace('campaign_id','').replace('device_id','').replace('referer','').replace(' | ','|').replace('| ','|').replace('\n','||\n').split('?')[1]
             line2 = line2.split('[')[1].split(']')[0].split(' ')[0] + '|'
	     line2 = line2 + line
	     file3.write(str(line2))


#Now the files are all set to operate in Spark

#Giving to DataFrames in Spark the data inside the files

df = spark.read.format("com.databricks.spark.csv") \
     .option("header", "false") \
     .option("delimiter", ",") \
     .option("inferSchema", "true") \
     .load(dir_files + "customer_leads_funnel.csv")

df_f_parse = spark.read.format("com.databricks.spark.csv") \
     .option("header", "false") \
     .option("delimiter", "|") \
     .option("inferSchema", "true") \
     .load(dir_files + "facebook_parse.csv")

df_g_parse = spark.read.format("com.databricks.spark.csv") \
     .option("header", "false") \
     .option("delimiter", "|") \
     .option("inferSchema", "true") \
     .load(dir_files + "google_parse.csv")

df_g_json = spark.read.json(dir_files + "google_ads_media_costs.jsonl")

df_f_json = spark.read.json(dir_files + "facebook_ads_media_costs.jsonl")

# 2.TRANSFORM DATA

print(' STEP 2: Transforming the files...')

# Creating the final DataFrame using the Google DataFrames, for import in Data Base
df_g_final = df.join(df_g_parse, df._c0 == df_g_parse._c3)\
               .join(df_g_json, (df_g_parse._c2 == df_g_json.google_campaign_id) & (df_g_parse._c1 == df_g_json.ad_creative_id) & (df_g_parse._c0 == df_g_json.date),"inner")\
               .select(df._c0.alias("device_id"),df._c1.alias("lead_id"),df._c2.alias("registered_at"),df._c3.alias("credit_decision"),df._c4.alias("credit_decision_at"),df._c5.alias("signed_at"),df._c6.alias("revenue"),df_g_json.ad_creative_id,df_g_parse._c2.alias("campaign_id"),df_g_parse._c4.alias("source"),df_g_json.ad_creative_name,df_g_json.google_campaign_name.alias("campaign_name"),df_g_json.clicks,df_g_json.cost,df_g_json.impressions).distinct()

# Creating the final DataFrame using the Facebook DataFrames, for import in Data Base
df_f_final = df.join(df_f_parse, df._c0 == df_f_parse._c2)\
               .join(df_f_json, (df_f_parse._c1 == df_f_json.facebook_campaign_id) & (df_f_parse._c0 == df_f_json.date),"inner")\
               .select(df._c0.alias("device_id"), df._c1.alias("lead_id"), df._c2.alias("registered_at"), df._c3.alias("credit_decision"), df._c4.alias("credit_decision_at"), df._c5.alias("signed_at"), df._c6.alias("revenue"), df_f_parse._c5.alias('ad_creative_id'),df_f_parse._c1.alias('campaign_id'), df_f_parse._c3.alias('source'), df_f_parse._c4.alias('ad_creative_name'),df_f_json.facebook_campaign_name.alias("campaign_name"), df_f_json.clicks, df_f_json.cost, df_f_json.impressions).distinct()

# 3.LOAD DATA

print(' STEP 3: Loading data in PostgreSQL...')

#Setting the var's to import the DataFrames in Data Base

jdbc_url = "jdbc:postgresql://127.0.0.1:5432/postgres"
mode = 'append'
table = 'campaign_analytics'
properties = {"user": "postgres",
              "password": "bcdias123",
              "driver": "org.postgresql.Driver"}

#Load the DataFrames in PostgreSQL

df_f_final.write.jdbc(jdbc_url, table, mode, properties)
df_g_final.write.jdbc(jdbc_url, table, mode, properties)

print('The data has been loaded !')
print('Closing Spark, see ya :D !')
exit()
#END
