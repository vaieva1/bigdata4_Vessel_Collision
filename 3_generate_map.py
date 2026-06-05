import pandas as pd
import matplotlib.pyplot as plt

def generate_map():
    # collision data
    MMSI_1 = "219021240" 
    MMSI_2 = "232018267"
    COLL_LAT = 55.223067 
    COLL_LON = 14.24373
    COLL_TIME_STR = "13/12/2021 02:27:29" 

    print("Loading filtered dataset...")
    df = pd.read_csv("filtered_december_ais.csv", usecols=["MMSI", "Latitude", "Longitude", "# Timestamp"])

    print("Cleaning MMSI...")
    # Convert to string, strip invisible spaces, and remove '.0'
    df["MMSI"] = df["MMSI"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    print("Extracting the two crashed ships...")
    ship1 = df[df["MMSI"] == MMSI_1].copy()
    ship2 = df[df["MMSI"] == MMSI_2].copy()

    # DIAGNOSTIC CHECK 1: Did we find them in the raw data?
    print(f" -> Found {len(ship1)} total rows for Ship 1 in the dataset.")
    print(f" -> Found {len(ship2)} total rows for Ship 2 in the dataset.")

    print("Converting timestamps (using lenient parsing)...")
    # dayfirst=True is safer than format string
    ship1["# Timestamp"] = pd.to_datetime(ship1["# Timestamp"], dayfirst=True, errors='coerce')
    ship2["# Timestamp"] = pd.to_datetime(ship2["# Timestamp"], dayfirst=True, errors='coerce')

    print("Applying 20-minute window constraint...")
    coll_time = pd.to_datetime(COLL_TIME_STR, dayfirst=True)
    start_window = coll_time - pd.Timedelta(minutes=10)
    end_window = coll_time + pd.Timedelta(minutes=10)

    ship1 = ship1[(ship1["# Timestamp"] >= start_window) & (ship1["# Timestamp"] <= end_window)]
    ship2 = ship2[(ship2["# Timestamp"] >= start_window) & (ship2["# Timestamp"] <= end_window)]

    # DIAGNOSTIC CHECK 2: Did they survive the time window?
    print(f" -> Ship 1 rows in the 20-min window: {len(ship1)}")
    print(f" -> Ship 2 rows in the 20-min window: {len(ship2)}")

    print("Sorting chronologically and dropping duplicates...")
    ship1 = ship1.sort_values(by="# Timestamp").drop_duplicates(subset=["# Timestamp"])
    ship2 = ship2.sort_values(by="# Timestamp").drop_duplicates(subset=["# Timestamp"])

    print("Drawing the map...")
    plt.figure(figsize=(10, 8))

    # Force full numbers on the axes, no scientific notation offsets
    plt.ticklabel_format(useOffset=False, style='plain')

    # Draw the star FIRST (zorder=3) so it sits in the background
    plt.plot(COLL_LON, COLL_LAT, marker='*', color='orange', markersize=25, label='COLLISION POINT', zorder=3)

    # formatting 1
    if not ship1.empty:
        plt.plot(ship1["Longitude"], ship1["Latitude"], 
                 color='blue', marker='o', markersize=4, linestyle='-', linewidth=2, alpha=0.9, label=f'Ship 1 (MMSI: {MMSI_1})', zorder=5)
    
    if not ship2.empty:
        plt.plot(ship2["Longitude"], ship2["Latitude"], 
                 color='red', marker='o', markersize=4, linestyle='-', linewidth=2, alpha=0.9, label=f'Ship 2 (MMSI: {MMSI_2})', zorder=5)

    # formatting 2
    plt.title("Vessel Collision Trajectory (20-Minute Window) \n 13/12/2021 02:27:29", fontsize=16, fontweight='bold')
    plt.xlabel("Longitude", fontsize=12)
    plt.ylabel("Latitude", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(fontsize=12)

    plt.savefig("real_collision_map.png", dpi=300, bbox_inches='tight')
    print("\nSuccess - open 'real_collision_map.png' to see crash paths.")

if __name__ == "__main__":
    generate_map()
