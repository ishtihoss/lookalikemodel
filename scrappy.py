# Scrappy Code for Lookalike dataprep
from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
from pyspark.sql.functions import month
import pygeohash as pgh


# Step 1: Ingest all MY data

df = spark.read.parquet('s3a://ada-prod-data/etl/service/boost/daily/MY/2021*')

# flatten data

ex_df = df.select('ifa','data_date','event_date','event_hour','location_name',\
    'parent_location_name','flag','location_count','transaction_count','transaction_amount_sum','event_details',F.explode('event_details'))

ex_df1 = ex_df.select('ifa','data_date','event_date','event_hour','location_name',\
    'parent_location_name','flag','location_count','transaction_count','transaction_amount_sum','event_details' ,col("col.*")).drop('event_details')

# Ingest pb data

pb_path = path = 's3a://ada-pb/MY/202102/WPPOI_MYS.txt'
df_pb = spark.read.option("sep", "|").option("header", "true").csv(pb_path)

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

# generate month column

df_ff = df_ff.withColumn('month',month(df_ff.event_date))

# Label people who eat fast food 3 or more times a month as fast food lovers (group by and count)

df_fa = df_ff.groupBy('ifa','NAME','month').count()


# You have achieved binary classifaction of fastfood lover or not [0, 1]

df_x = df_fa.withColumn('freq_ffc', F.when(F.col('count') >= 3, 1).otherwise(0))
df_y = df_x.select('ifa','freq_ffc')

df_z = df_res.join(df_y, on='ifa', how='left')

# add persona data to df_z

df_p = spark.read.parquet('s3a://ada-dev/DA_repository/Persona/MY/*/2021*/ifa_list/')

df_z1 = df_z.join(df_p, on='ifa', how='left')

# enrich data with affluence

path_af = 's3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/MY/2021*'
df_af = spark.read.parquet(path_af).select('ifa','final_score')

df_z2 = df_z1.join(df_af, on='ifa', how='left')


# enrich data with lifestange information

ls_path = 's3a://ada-platform-components/lifestages/monthly_predictions/MY/*/2021*/ifa_list/'
df_ls = spark.read.parquet(ls_path).select('ifa','lifestage_name_m1','lifestage_name_m2')

df_z3 = df_z2.join(df_ls, on='ifa', how='left')
# Step 2
df_z3.where(F.col('freq_ffc').isNull())
Enrich data with affluence, life stage and persona

# Step 3

Time to try some kind of classification algorithm (within cosmose data)

This will help: https://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.ml.html
