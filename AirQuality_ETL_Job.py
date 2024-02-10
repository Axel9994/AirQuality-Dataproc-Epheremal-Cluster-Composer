from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import col
from pyspark.sql.functions import year, month, dayofmonth, hour
from pyspark.sql.functions import avg

FILE_BUCKET_NAME = 'TU_BUCKET_DE_ARCHIVOS'
TEMP_BUCKET_NAME = 'BUCKET_TEMPORAL'
DEST_TABLE = 'ID_PROYECTO.TU_DATASET.airquality_silver_layer_data'

spark = SparkSession.builder \
  .appName('AirQuality Epheremal Cluster Cloud Composer') \
  .getOrCreate()


weather_df = spark \
  .read \
  .option ( "inferSchema" , "true" ) \
  .option ( "header" , "true" ) \
  .csv ( "gs://{0}/*_bme280sof.csv".format(FILE_BUCKET_NAME) )

pollution_df = spark \
    .read \
    .option ( "inferSchema" , "true" ) \
    .option ( "header" , "true" ) \
    .csv("gs://{0}/*_sds011sof.csv".format(FILE_BUCKET_NAME) )


cols = ['Index', '_c0', 'sensor_id'] 
weather_df = weather_df.drop(*cols) 
pollution_df = pollution_df.drop(*cols)

airquality_df = weather_df.alias("w").join(pollution_df.alias("p"), (weather_df.location == pollution_df.location) & (weather_df.timestamp == pollution_df.timestamp), "inner")\
    .select("w.lat", "w.lon", "w.timestamp", "w.pressure", "w.temperature", "w.humidity", "p.P1", "p.P2")

airquality_df = airquality_df.withColumn("Pollution_total", col('P1') + col('P2'))
airquality_df = airquality_df.drop(col('P1')) 
airquality_df = airquality_df.drop(col('P2')) 

airquality_df = airquality_df.filter("lat is not null AND lon is not null AND timestamp is not null AND pressure is not null AND temperature is not null AND Pollution_total is not null") 

airquality_df = airquality_df \
    .withColumn("year", year(col('timestamp'))) \
    .withColumn("month", month(col('timestamp'))) \
    .withColumn("day", dayofmonth(col('timestamp'))) \
    .withColumn("hour", hour(col('timestamp'))) 

temperature_df = airquality_df.groupBy(["month", "day", "hour"]) \
    .agg(avg("temperature").alias("average_temperature")) 

airquality_df = airquality_df.alias("a").join(temperature_df.alias("t"), (airquality_df.month == temperature_df.month) & (airquality_df.day == temperature_df.day) & (airquality_df.hour == temperature_df.hour), "inner") \
    .select("a.lat", "a.lon", "a.timestamp", "a.pressure", "t.average_temperature", "a.humidity", "a.Pollution_total", "a.year", "a.month", "a.day", "a.hour") 

airquality_df \
    .write \
    .format('bigquery') \
    .option('temporaryGcsBucket', TEMP_BUCKET_NAME) \
    .option('table', DEST_TABLE) \
    .mode('overwrite') \
    .save()