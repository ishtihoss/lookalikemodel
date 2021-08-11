##### Process Lookalike Data ######

# standard libraries

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
import pygeohash as pgh

# Ingest boost data
path = 's3a://ada-prod-data/etl/service/boost/daily/MY/2021*'
df = spark.read.parquet(path)

# Flatten the df with explode so that later it can be joined on geohash with pb2


ex_df = df.select('ifa','data_date','event_date','event_hour','location_name',\
    'parent_location_name','flag','location_count','transaction_count','transaction_amount_sum','event_details',F.explode('event_details'))

ex_df1 = ex_df.select('ifa','data_date','event_date','event_hour','location_name',\
    'parent_location_name','flag','location_count','transaction_count','transaction_amount_sum','event_details' ,col("col.*")).drop('event_details')


# generate geohash from boost data

# Ingest pb data

pb_path = path = 's3a://ada-pb/MY/202102/WPPOI_MYS.txt'
df_pb = spark.read.option("sep", "|").option("header", "true").csv(pb_path)

# Generate geohash for PB data (note the level of precision)

# Cast lat long as float

df_pb = df_pb.withColumn('LATITUDE',F.col('LATITUDE').cast(T.FloatType())) \
    .withColumn('LONGITUDE',F.col('LONGITUDE').cast(T.FloatType()))

# Run through a user defined function

udf1 = F.udf(lambda x,y: pgh.encode(x,y,precision=9))
df_pb = df_pb.withColumn('geohash9',udf1('LATITUDE','LONGITUDE'))

# select the important fields from df_pb

df_pb1 = df_pb.select("NAME", "BRANDNAME", "TRADE_NAME", "FRANCHISE_NAME", "BUSINESS_LINE", "SIC8_DESCRIPTION", "TRADE_DIVISION", "GROUP_NAME", "MAIN_CLASS", "SUB_CLASS", "LATITUDE", "LONGITUDE", "geohash9")

# Escape utf encoding error

my_udf = F.udf(lambda x: x.encode().decode('unicode-escape'),T.StringType())
df_pb2 = df_pb1.withColumn('NAME', my_udf('NAME'))

# join pitney bowes data with original cosmose data

jdf = ex_df1.join(df_pb2, ex_df1.loc_geohash==df_pb2.geohash9, how='left')

# pull BK customers

bk_df  = jdf.filter(F.col('NAME')=="BURGER KING") # Only 76 rows. Ingest more Cosmose.

vist_count = bk_df.groupBy('ifa','NAME').count()

bk_df1 = bk_df.join(visit_count,on='ifa',how='left').orderBy('count', ascending=False)