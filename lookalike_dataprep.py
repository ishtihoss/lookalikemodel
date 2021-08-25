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
from pyspark.ml.fpm import FPGrowth

# Ingest boost data
path = 's3a://ada-prod-data/etl/service/boost/daily/MY/202101*'
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

# restaurants only

df_res = jdf.filter(F.col('MAIN_CLASS')=='EATING PLACES/RESTAURANTS')

# Take fastfood segment

df_ff = df_res.filter(F.col('SIC8_DESCRIPTION')=='FAST FOOD RESTAURANTS AND STANDS')

# create set-list function to be used in a UDF

def set_items(items):
    item_set = set(items)
    return list(item_set)

# agg data for passing on to UDF

df_prep = df_ff.groupBy('ifa').agg(F.collect_set(df_ff.NAME).alias('NAME'))
df_prep = df_prep.select('NAME')
udf_set_items = F.udf(set_items, T.ArrayType(T.StringType(), True))
df_prep = df_prep.withColumn('items',udf_set_items(df_prep.NAME)).drop('NAME')

# replace individual fast food restaurant name with generic label "fast food"

df_p = df_res.withColumn('gen_name',\
    F.when(F.col('SIC8_DESCRIPTION')=='FAST FOOD RESTAURANTS AND STANDS', \
    "Fast Food Lover").otherwise(F.col("NAME"))).drop('NAME')

df_prep1 = df_p.groupBy('ifa').agg(F.collect_set(df_p.gen_name).alias('NAME'))
df_prep1 = df_prep1.select('NAME')
udf_set_items = F.udf(set_items, T.ArrayType(T.StringType(),True))
df_prep1 = df_prep1.withColumn('items',udf_set_items(df_prep1.NAME)).drop('NAME')

fp = FPGrowth(minSupport=0.02, minConfidence=0.3)
fpm = fp.fit(df_prep1)
fpm.freqItemsets.show(5)















#combine = df_res.groupBy('ifa', 'NAME').count()
#cc = combine.groupBy('ifa').pivot('NAME').agg({'count':'max'}) # Pivot fails because 'NAME' col has too many distinct values

# pivot the data

# jdf.groupBy('ifa').pivot('NAME').show(10)  # doesn't work because too many distinct values
udf_set_items = F.udf(set_items, T.ArrayType(T.StringType(),True))
gg = jdf.withColumn('items', udf_set_items(jdf.NAME)).drop('NAME')







# pull BK customers

bk_df  = jdf.filter(F.col('NAME')=="BURGER KING") # Only 76 rows. Ingest more Cosmose.

ifa_list = bk_df.select('ifa').rdd.flatMap(lambda x: x).collect()

gg = jdf.where( ( F.col('ifa').isin (ifa_list)) )

gg3 = gg2.groupBy('ifa').pivot('NAME').agg({'count':'max'})


SIC8_DESCRIPTION='FAST FOOD RESTAURANTS AND STANDS'


vist_count = bk_df.groupBy('ifa','NAME').count()

bk_df1 = bk_df.join(visit_count,on='ifa',how='left').orderBy('count', ascending=False)
