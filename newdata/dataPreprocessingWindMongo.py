

#Entrypoint 2.x
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, DateType
from util import check_record_exist

spark = SparkSession.builder.appName("City Wind Data Preprocessing ") \
.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/weather.wind") \
.config("spark.mongodb.input.uri","mongodb://127.0.0.1:27017/weather.wind") \
.getOrCreate()

sc = spark.sparkContext

schema = StructType([
    StructField("properties", StructType([
        StructField("parameter", StructType([
            StructField("WD10M", MapType(StringType(),DoubleType()),True),
            StructField("WS10M", MapType(StringType(),DoubleType()),True),
            StructField("WD2M",  MapType(StringType(),DoubleType()),True),
        ]), True)
    ]), True),
    StructField("geometry", StructType([
        StructField("coordinates", ArrayType(DoubleType()),True)
    ]), True)
])


city_df = spark.read.csv('file:///home/hadoop/Documents/Indian_Cities_Database.csv', header=True)

for city in city_df.collect():      
    # Load the json file
    file_path = f'file:///home/hadoop/Documents/newdata/wind/{city["City"]}_{city["latitude"]}_{city["longitude"]}_*.json'

    data_df = spark.read.json(file_path, multiLine=True,schema=schema) \
    .select(F.col("properties.parameter.*"), F.col("geometry.coordinates"))


    # create dataframe for features "PS","PSC","T2M","T2MWET","T2MDEW" 
    from pyspark.sql.functions import explode
    WD2M =  data_df.select('coordinates',explode(data_df.WD2M).alias("Date", "WD2M"))
    WD10M = data_df.select(explode(data_df.WD10M).alias("Date", "WD10M"))
    WS10M = data_df.select(explode(data_df.WS10M).alias("Date", "WS10M"))
    #"WD10M",WS10M, WD2M

    from pyspark.sql.functions import desc
    wind_df = WD2M.join(WD10M, WD2M.Date == WD10M.Date, 'inner') \
    .join(WS10M , WD2M.Date == WS10M.Date, 'inner') \
    .select(WD2M.Date, "WD2M","WD10M","WS10M") \
    .withColumn("City",F.lit(city["City"]))

    # another approach is by renaming.
	# final_df.show()
	
    # Aggregate the values of the 'Date' column into a list for each partition
    #date_values = wind_df.groupby().agg(F.collect_list('Date')).first()[0]
    #city_values = wind_df.groupby().agg(F.collect_list('City')).first()[0]
	
	# Filter DataFrame to keep only rows that don't exist in MongoDB
    #wind_df_filtered = wind_df.filter(~check_record_exist("wind",Date=F.collect_list('Date'),City=F.collect_list('City')))
    
    # using python mongo 
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017/")
    db = client.weather

    
    for row in wind_df.collect():
        if db.wind.find_one({'Date': row['Date'], 'City': row['City']}):
            print(f"Data with Date '{row['Date']}' already exists. Skipping...")
        else:
           db.wind.insert_one(row.asDict())


	# Write filtered DataFrame to MongoDB
    #wind_df_filtered.write \
	#.format("com.mongodb.spark.sql.DefaultSource") \
	#.mode("append") \
	#.save()

    
spark.stop()
