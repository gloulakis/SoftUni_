# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# Weather

# CELL ********************

Weather

# Welcome to your new notebook
# Type here in the cell editor to add code!
# ========= Weather → Fabric Lakehouse (Delta) | End-to-End Fixed (06–22 avg, min, max) =========

from pyspark.sql import SparkSession, functions as F, types as T
import requests, time, random, datetime as dt
from dateutil.relativedelta import relativedelta

LakehouseTargetPath = "edc337s23y6u5mpheug4mk6n64-yzxry3oeliruffxj5bpf3uwubq.datawarehouse.fabric.microsoft.com"
target_table_path = f"{LakehouseTargetPath}/Weather"

START_DATE = "2024-01-01"
# ← only through yesterday
END_DATE   = (dt.date.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")

WINDY_THRESHOLD = 8.0

REGIONS = {
    "Sofia":       (42.6977, 23.3219),
    "Plovdiv":     (42.1354, 24.7453),
    "Burgas":      (42.5048, 27.4626),
    "Pazardzhik":  (42.1928, 24.3339),
    "Sliven":      (42.6810, 26.3229),
    "Yambol":      (42.4833, 26.5000)
}

session = requests.Session()
session.headers.update({"User-Agent": "fabric-weather/1.0"})

def month_ranges(start_date, end_date):
    s = dt.date.fromisoformat(start_date)
    e = dt.date.fromisoformat(end_date)
    cur = s.replace(day=1)
    while cur <= e:
        nxt = (cur + relativedelta(months=1)) - dt.timedelta(days=1)
        yield max(cur, s).strftime("%Y-%m-%d"), min(nxt, e).strftime("%Y-%m-%d")
        cur = (cur + relativedelta(months=1)).replace(day=1)

def fetch_with_retry(params, retries=4, base_sleep=1.5, timeout=180):
    url = "https://archive-api.open-meteo.com/v1/archive"
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.ok:
                return r.json()
        except requests.RequestException:
            pass
        if i < retries - 1:
            time.sleep(base_sleep * (2 ** i) + random.random())
    return None

daily_vars = ["temperature_2m_mean","precipitation_sum","windspeed_10m_mean",
              "snowfall_sum","precipitation_hours","cloudcover_mean"]
hourly_vars = ["temperature_2m"]

daily_records, hourly_rows = [], []

for region, (lat, lon) in REGIONS.items():
    for start, end in month_ranges(START_DATE, END_DATE):
        params = {
            "latitude": lat, "longitude": lon,
            "start_date": start, "end_date": end,
            "daily": ",".join(daily_vars),
            "hourly": ",".join(hourly_vars),
            "timezone": "Europe/Sofia",
        }
        data = fetch_with_retry(params)
        if not data:
            continue

        if "daily" in data:
            d = data["daily"]
            for i, day in enumerate(d.get("time", [])):
                rec = {
                    "region": region, "date": day,
                    "temp_avg": int(d["temperature_2m_mean"][i]),
                    "precip_mm": int(d["precipitation_sum"][i]),
                    "wind_avg": int(d["windspeed_10m_mean"][i]),
                    "snow_mm": int(d["snowfall_sum"][i]),
                    "precip_hours": int(d["precipitation_hours"][i]),
                    "cloud_cover": int(d["cloudcover_mean"][i]),
                }
                rec["is_rainy"] = bool((rec["precip_mm"] or 0) > 0)
                rec["is_windy"] = bool((rec["wind_avg"] or 0) > WINDY_THRESHOLD)
                rec["is_snowy"] = bool((rec["snow_mm"] or 0) > 0)
                rec["is_sunny"] = bool((rec["precip_mm"] or 0) == 0 and (rec["cloud_cover"] or 0) < 30)
                daily_records.append(rec)

        if "hourly" in data and data["hourly"].get("temperature_2m"):
            for t, temp in zip(data["hourly"]["time"], data["hourly"]["temperature_2m"]):
                hourly_rows.append((region, t, int(temp)))

spark = SparkSession.builder.appName("Fabric_Weather_Ingest").getOrCreate()

df_daily = spark.createDataFrame(daily_records).withColumn("date", F.to_date("date"))

df_hourly = (
    spark.createDataFrame(hourly_rows, ["region","ts_str","temp_hour"])
    .withColumn("ts", F.to_timestamp("ts_str"))
    .withColumn("date", F.to_date("ts"))
    .withColumn("hour", F.hour("ts"))
)

df_6_22 = (
    df_hourly
    .filter((F.col("hour") >= 6) & (F.col("hour") <= 22))
    .groupBy("region","date")
    .agg(
        F.avg("temp_hour").alias("temp_avg_6_22"),
        F.min("temp_hour").alias("temp_min_6_22"),
        F.max("temp_hour").alias("temp_max_6_22")
    )
)

df_final = df_daily.join(df_6_22, ["region","date"], "left")

(df_final.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save(target_table_path))

print(f"✔ Weather written to Delta: {target_table_path} | rows={df_final.count()}")
df_final.orderBy("region","date").show(10, truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
