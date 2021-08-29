"""
Question 1 :

A Diesel Generator sends data to our platform along with events like power on , fuel level etc. Power status events are stores in one table and fuel level information is stored in one table.

Below are the few aspects i have considered :

Considered one session is equal to DG ON to OFF cycle.
Have converted tmsInLong data from miliseconds to seconds first, before coverting the epoch timestamp to human-readable timestamp.
df_fuel_consumption_per_session_per_day - gives us the fuel consumption during every session through the day.
df_fuel_consumption_total_per_day_in_hr - generates a Daily report(consumption per day in hours)


"""
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import types as t
from pyspark.sql import functions as f

conf = SparkConf()
spark = SparkSession.builder.config(conf=conf).getOrCreate()

fuel_data = "/home/prasson/PycharmProjects/jioRound2/sample_data/Fuel_Consumption"
power_data = "/home/prasson/PycharmProjects/jioRound2/sample_data/Power_Event"

df_fuel = spark.read.format("csv").option("delimiter", "|").option("header", "true").\
    option("inferSchema", "true").load(fuel_data)

df_power = spark.read.format("csv").option("delimiter", "|").option("header", "true").\
    option("inferSchema", "true").load(power_data)

df_power_time = df_power.withColumn("time", f.to_timestamp(f.col("tmsInLong")/1000))
df_fuel_time = df_fuel.withColumn("time", f.to_timestamp(f.col("tmsInLong")/1000))

# power
windowSpec = Window.partitionBy("asset_identifier").orderBy("time")
df_power_lag = df_power_time.withColumn("lag_time_diff", f.lag("time", 1).over(windowSpec))
#df_power_lag.show(truncate=False)

# 1.A - Generate the run time of the DG per session per day
df_power_runtime_per_session = df_power_lag.withColumn("DG_runtime", f.col("time").cast("long") - f.col('lag_time_diff').cast("long")).\
    where(f.col("event") == 'OFF')
#df_power_runtime_per_session.show(truncate=False)

# 1.B - Generate the run time of the DG per day
df_power_runtime_per_day = df_power_runtime_per_session.groupBy("asset_identifier", "tmsdate").\
    agg(f.sum("DG_runtime").alias("DG_total_run_per_day"))
#df_power_runtime_per_day.show(truncate=False)

# fuel - Generate fuel consumed by the DG
df_fuel_cast = df_fuel_time.withColumn("ltr", df_fuel_time["ltr"].cast(t.FloatType()))
df_fuel_con_per_day = df_fuel_cast.groupBy("asset_identifier", "tmsdate").agg((f.max("ltr") - f.min("ltr")).alias("ltr_consumption"))

# 1.A - Calculate fuel consumption during every session through the day
df_fuel_consumption_per_session_per_day = df_power_runtime_per_session.join(df_fuel_con_per_day,"asset_identifier", "INNER")
df_fuel_consumption_per_session_per_day.select("asset_identifier", "type", "DG_runtime", "ltr_consumption").show(truncate=False)

# 1.B - Generate a Daily report for fuel consumption per hour on daily basis.
df_fuel_consumption_total_per_day = df_power_runtime_per_day.join(df_fuel_con_per_day,"asset_identifier", "INNER")

# df_fuel_consumption_total_per_day.show(truncate=False)
df_fuel_consumption_total_per_day_in_hr = df_fuel_consumption_total_per_day.\
    withColumn("DG_runtime_in_hr", df_fuel_consumption_total_per_day["DG_total_run_per_day"]/3600)
df_fuel_consumption_total_per_day_in_hr.select("asset_identifier", df_fuel_con_per_day["tmsdate"],
                                              "DG_runtime_in_hr", "ltr_consumption").show(truncate=False)
