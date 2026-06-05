'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, unix_timestamp, pow, cos, radians, lit, sqrt, lag, count, concat_ws
from pyspark.sql.window import Window

def main():
    print("Starting PySpark Session...")
    spark = SparkSession.builder \
        .appName("VesselCollisionDetector") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print("Loading filtered CSV...")
    df = spark.read.csv("filtered_december_ais.csv", header=True, inferSchema=True)
    df = df.dropna(subset=["Latitude", "Longitude", "MMSI", "# Timestamp", "SOG"])

    # 1. Filter out stationary vessels (as required by the rubric)
    df = df.filter(col("SOG") > 0.5)

    print("Applying Assignment 1 Inspired GPS Anomaly Filter (Max 40 Knots)...")
    window_spec = Window.partitionBy("MMSI").orderBy(unix_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss"))
    
    # Calculate previous lat/lon and time
    df = df.withColumn("prev_lat", lag("Latitude").over(window_spec)) \
           .withColumn("prev_lon", lag("Longitude").over(window_spec)) \
           .withColumn("prev_time", lag(unix_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss")).over(window_spec))
    
    # Calculate distance in Nautical Miles (1 degree lat = ~60 NM)
    df = df.withColumn("dlat_nm", (col("Latitude") - col("prev_lat")) * 60) \
           .withColumn("dlon_nm", (col("Longitude") - col("prev_lon")) * cos(radians(col("Latitude"))) * 60) \
           .withColumn("dist_nm_jump", sqrt(pow(col("dlat_nm"), 2) + pow(col("dlon_nm"), 2)))
    
    # Calculate Time difference in Hours
    df = df.withColumn("time_diff_hours", (unix_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss") - col("prev_time")) / 3600.0)

    # Calculate Speed in Knots and Filter Out Impossible Jumps (>40 knots)
    df = df.withColumn("calc_speed_knots", col("dist_nm_jump") / col("time_diff_hours"))
    df = df.filter((col("time_diff_hours").isNull()) | (col("calc_speed_knots") <= 40.0))

    print("Applying Trajectory Density Filter...")
    # Ensure a ship has a robust track (at least 20 valid pings) to avoid 1-point ghosts
    track_counts = df.groupBy("MMSI").count().filter(col("count") >= 20)
    df = df.join(track_counts, "MMSI")

    print("Applying 50nm radius constraint...")
    CENTER_LAT, CENTER_LON, RADIUS_NM = 55.225000, 14.245000, 50.0
    df = df.withColumn("dist_to_center_nm", sqrt(pow(col("Latitude") - lit(CENTER_LAT), 2) + 
            pow(cos(radians(lit(CENTER_LAT))) * (col("Longitude") - lit(CENTER_LON)), 2)) * 60) \
           .filter(col("dist_to_center_nm") <= RADIUS_NM)

    print("Spatial Indexing & Join...")
    # Tighter grid and time bin (1 minute) to ensure they are physically touching
    df = df.withColumn("grid_lat", spark_round(col("Latitude"), 3)) \
           .withColumn("grid_lon", spark_round(col("Longitude"), 3)) \
           .withColumn("time_bin", spark_round(unix_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss") / 60))

    df_a, df_b = df.alias("a"), df.alias("b")
    collisions = df_a.join(df_b, (col("a.grid_lat") == col("b.grid_lat")) & 
        (col("a.grid_lon") == col("b.grid_lon")) & (col("a.time_bin") == col("b.time_bin")) & 
        (col("a.MMSI") < col("b.MMSI")))

    print("Removing Convoys (Loitering/Towing pairs)...")
    # A collision is an accident. A convoy shares the same grid for hours.
    collisions = collisions.withColumn("pair_id", concat_ws("_", col("a.MMSI"), col("b.MMSI")))
    window_pair = Window.partitionBy("pair_id")
    collisions = collisions.withColumn("encounter_length", count("pair_id").over(window_pair))
    # Filter out ships that travel together for more than 5 minutes (5 time bins)
    valid_collisions = collisions.filter(col("encounter_length") <= 5)

    print("Extracting the Closest Physical Impact...")
    best_match = valid_collisions.withColumn("dist_sq", pow(col("a.Latitude") - col("b.Latitude"), 2) + 
                                       pow(col("a.Longitude") - col("b.Longitude"), 2)) \
                           .orderBy("dist_sq") \
                           .select(
                               col("a.MMSI").alias("Ship_1_MMSI"),
                               col("b.MMSI").alias("Ship_2_MMSI"),
                               col("a.# Timestamp").alias("Time"),
                               col("a.Latitude"),
                               col("a.Longitude")
                           ).limit(1).collect()

    if best_match:
        crash = best_match[0]
        print("\n====================")
        print("COLLISION DETECTED")
        print("====================")
        print(f"Ship 1 MMSI: {crash['Ship_1_MMSI']}")
        print(f"Ship 2 MMSI: {crash['Ship_2_MMSI']}")
        print(f"Time: {crash['Time']}")
        print(f"Coordinates: {crash['Latitude']}, {crash['Longitude']}")
        print("====================")
    else:
        print("No valid collision detected after cleaning.")

    spark.stop()

if __name__ == "__main__":
    main()
    '''

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, round as spark_round, unix_timestamp, pow, cos, sin, asin, radians, lit, sqrt, abs as spark_abs, lead, min as spark_min, max as spark_max
)
from pyspark.sql.window import Window

def main():
    print("Starting PySpark Session...")
    spark = SparkSession.builder \
        .appName("VesselCollisionDetector") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    print("Loading filtered CSV...")
    df = spark.read.csv("filtered_december_ais.csv", header=True, inferSchema=True)
    df = df.dropna(subset=["Latitude", "Longitude", "MMSI", "# Timestamp", "SOG"])
    df = df.withColumn("unix_time", unix_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss"))

    print("Applying 'Signal Death' Sensor Logic (Anomaly A)...")
    # true collision most probably destroys transponder. We flag the exact ping where a ship goes dark
    window_time = Window.partitionBy("MMSI").orderBy("unix_time")
    df = df.withColumn("next_ping_time", lead("unix_time").over(window_time))
    # If the ship goes silent for > 4 hours, or this is its final ping forever, mark as True
    df = df.withColumn("goes_dark", (col("next_ping_time") - col("unix_time") > 14400) | col("next_ping_time").isNull())

    print("Applying High-Speed Transit Filters...")
    # Both ships must be traveling at transit speeds (> 4.0 knots) to cause a catastrophe
    df = df.filter(col("SOG") >= 4.0)

    print("Applying 50nm Geographic Constraint & Ghost Ping Killer...")
    # Keep only ships actually in the zone
    CENTER_LAT, CENTER_LON = 55.225000, 14.245000
    a_center = pow(sin(radians(col("Latitude") - lit(CENTER_LAT)) / 2), 2) + \
               cos(radians(lit(CENTER_LAT))) * cos(radians(col("Latitude"))) * \
               pow(sin(radians(col("Longitude") - lit(CENTER_LON)) / 2), 2)
    c_center = 2 * asin(sqrt(a_center))
    df = df.withColumn("dist_to_center_nm", lit(3440.0) * c_center)
    df = df.filter(col("dist_to_center_nm") <= 50.0)
    
    # Must have at least 20 pings in the zone (kills 1-point teleporting glitches)
    track_counts = df.groupBy("MMSI").count().filter(col("count") >= 20)
    df = df.join(track_counts, "MMSI")

    print("Spatial Indexing (15-Minute 'Approach' Bins)...")
    # We use a broad grid (1 decimal = ~11km) and 15-minute bins so we can watch them converge
    df = df.withColumn("grid_lat", spark_round(col("Latitude"), 1)) \
           .withColumn("grid_lon", spark_round(col("Longitude"), 1)) \
           .withColumn("time_bin", spark_round(col("unix_time") / 900))

    df_a, df_b = df.alias("a"), df.alias("b")
    collisions = df_a.join(df_b, (col("a.grid_lat") == col("b.grid_lat")) & 
        (col("a.grid_lon") == col("b.grid_lon")) & (col("a.time_bin") == col("b.time_bin")) & 
        (col("a.MMSI") < col("b.MMSI")))

    print("Synchronizing Pings and Calculating Exact Distance...")
    # compare pings that happened within 60 seconds of each other
    sync_collisions = collisions.filter(spark_abs(col("a.unix_time") - col("b.unix_time")) <= 60)

    a_prox = pow(sin(radians(col("a.Latitude") - col("b.Latitude")) / 2), 2) + \
             cos(radians(col("a.Latitude"))) * cos(radians(col("b.Latitude"))) * \
             pow(sin(radians(col("a.Longitude") - col("b.Longitude")) / 2), 2)
    c_prox = 2 * asin(sqrt(a_prox))
    sync_collisions = sync_collisions.withColumn("dist_nm", lit(3440.0) * c_prox)

    print("Applying 'Convergence' Physics (User Custom Logic)...")
    # For each pair in a 15-minute window, what was their furthest distance and closest distance?
    pair_window = Window.partitionBy("a.MMSI", "b.MMSI", "a.time_bin")
    sync_collisions = sync_collisions.withColumn("min_dist", spark_min("dist_nm").over(pair_window))
    sync_collisions = sync_collisions.withColumn("max_dist", spark_max("dist_nm").over(pair_window))

    # 1. min_dist <= 0.1: They physically collided.
    # 2. max_dist - min_dist > 0.5: They converged from at least 0.5 nm away (Kills Convoys/Towing).
    # 3. goes_dark: At least one ship stopped transmitting right after (Kills Safe Transfers).
    final_candidates = sync_collisions.filter(
        (col("min_dist") <= 0.1) & 
        (col("max_dist") - col("min_dist") >= 0.5) & 
        (col("a.goes_dark") | col("b.goes_dark"))
    )

    # Get the exact moment of impact
    best_match = final_candidates.orderBy("min_dist").select(
        col("a.MMSI").alias("Ship_1_MMSI"),
        col("b.MMSI").alias("Ship_2_MMSI"),
        col("a.# Timestamp").alias("Time"),
        col("a.Latitude"),
        col("a.Longitude")
    ).limit(1).collect()

    if best_match:
        crash = best_match[0]
        print("\n====================")
        print("HISTORICAL COLLISION DETECTED")
        print("====================")
        print(f"Ship 1 MMSI: {crash['Ship_1_MMSI']}")
        print(f"Ship 2 MMSI: {crash['Ship_2_MMSI']}")
        print(f"Time: {crash['Time']}")
        print(f"Coordinates: {crash['Latitude']}, {crash['Longitude']}")
        print("====================")
    else:
        print("No valid collision detected after cleaning.")

    spark.stop()

if __name__ == "__main__":
    main()