# **Big Data Assignment #4: Detection of Vessel Collisions**

## **Description**

The objective of this assignment is to evaluate the processing of large-scale temporal and spatial data. The task requires identifying two vessels that collided within a specified marine area (50-nautical-mile radius) from a massive dataset, eliminating extreme data noise, and visualizing their trajectories 10 minutes prior to and 10 minutes following the time of collision.

**GitHub** repository – [https://github.com/vaieva1/bigdata4_Vessel_Collision](https://github.com/vaieva1/bigdata4_Vessel_Collision)

## **Input data**

Dataset used: [Danish Maritime Authority AIS data](http://aisdata.ais.dk/).

Timeframe used in the assignment: **December 1, 2021 – December 31, 2021**.

❗**Note**: The raw dataset (approx. 16GB) is not included in this repository to maintain a lightweight environment and align with standard data engineering practices. 

## Project Files
- `1_prefilter_data.py`: Local script to apply geographic bounding boxes.
- `2_find_collision.py`: PySpark application (runs in Docker) for spatial indexing and anomaly filtering.
- `3_generate_map.py`: Local script to plot the trajectory visualization.
- `Dockerfile` & `docker-compose.yml`: Containerization configurations.
- `REPORT.md`: Detailed methodology, findings, and trajectory visualizations.

## **Instructions on how to run the pipeline:**

**0. Prerequisites:**
Ensure you have Docker and Docker Compose installed. Download the `aisdk-2021-12.zip` file from the Danish Maritime Authority and place it in the root directory of this project.

**1. Pre-filter the raw data (local execution):**
To prevent memory overload, a rough geographic bounding box is applied to the raw CSV files. Open your terminal and run:
```bash
python 1_prefilter_data.py
```
*This extracts and filters the data, creating a much smaller `filtered_december_ais.csv` file.*

**2. Execute the collision detection (Containerized PySpark):**
The heavy data processing is handled by Apache Spark. Build and run the Docker container:
```bash
docker compose up --build
```
*The `docker-compose.yml` uses a volume mount to safely process the `filtered_december_ais.csv` without filling up the Docker daemon's memory. The terminal will eventually output the exact MMSI numbers, timestamp, and coordinates of the collision.*

**3. Generate the Trajectory Visualization (local execution):**
Once the PySpark job finishes, run the mapping script to visualize the 20-minute window of the crash:
```bash
python 3_generate_map.py
```
*This will generate and save `real_collision_map.png` in your directory.*

---

## **Project architecture and module description:**

- `1_prefilter_data.py`  
  A lightweight Python script utilizing `zipfile` and `csv` to stream the raw data. It filters out any rows outside the rough bounds of `LAT 54.0-56.5` and `LON 13.0-15.5`.

- `2_find_collision.py`  
  The core PySpark application that processes the dataset to detect the collision. To avoid a computationally impossible Cartesian product (comparing every ship to every other ship), it employs a multi-step data engineering pipeline:
  - **Data cleaning & "ghost ping" removal:** Filters out stationary vessels (SOG < 4.0 knots) and requires at least 20 valid pings per ship within the target 50nm radius to eliminate GPS teleportation glitches.
  - **Spatial indexing:** Groups the map into a rough 11km grid with 15-minute time bins, significantly reducing the computational load by only joining and comparing ships that share the exact same grid box and timeframe.
  - **Kinematic & sensor filters:** Calculates the exact Haversine distance to ensure ships physically converged from a distance (filtering out safe tugboat convoys), and applies a "Signal Death" rule requiring at least one ship's transponder to go dark immediately upon impact.

- `3_generate_map.py`  
  Uses `pandas` and `matplotlib` to isolate the specific MMSI numbers found by PySpark (values that appear in the output of `2_find_collision.py` and have to be filled here), filter the timeline to exactly +/- 10 minutes from the crash, and plot the trajectories.

- `Dockerfile` & `docker-compose.yml`  
  Sets up a `python:3.9-slim-bullseye` environment with `default-jre-headless` (Java 11) to ensure PySpark 3.4.1 runs smoothly without crashing.

---

## **Implementation details: filtering noisy data**

AIS data is quite messy. Simply searching for the two closest ships yields false positives like docked ships, pilot transfers, or data glitches. We implemented the following filters in `2_find_collision.py`:

### **1. Stationary vessels & safe transfers**
Filtered out any ships with a Speed Over Ground (SOG) of less than 4.0 knots:
```python
df = df.filter(col("SOG") >= 4.0)
```
This instantly removes anchored ships, safely docked vessels, and slow-speed transfers, ensuring we only evaluate active, higher-speed transits.

### **2. "Ghost ping" (teleportation) filter**
Sometimes a ship's GPS glitches, firing a single ping into the Baltic Sea while the ship is physically in another country. So forced the system to verify that a ship had a sustained presence (at least 20 pings) inside our 50-nautical-mile radius before considering it a valid vessel for this analysis.

### **3. "Convergence" physics (anti-convoy)**
For example tugboats towing barges will share the same exact coordinates for hours, so to filter out these safe convoys, I calculate `min_dist` and `max_dist` of an encounter within a 15-minute window. Also, required the ships to converge from a distance before hitting:
```python
(col("max_dist") - col("min_dist") >= 0.5) 
```

### **4. "Going dark" assumption**
A serious-to-catastrophic collision could very potentially destroy a ship's transponder. This assumption decision is described in more detail in `REPORT.md` file. So "signal death" filter is implemented, requiring at least one ship to stop transmitting data immediately after the encounter:
```python
window_time = Window.partitionBy("MMSI").orderBy("unix_time")
df = df.withColumn("next_ping_time", lead("unix_time").over(window_time))
df = df.withColumn("goes_dark", (col("next_ping_time") - col("unix_time") > 14400) | col("next_ping_time").isNull())
```
---

## **Results & visualization**

The code command 

```bash
docker compose up --build
```
successfully isolated a collision:
* **Ship 1:** MMSI 219021240
* **Ship 2:** MMSI 232018267
* **Collision Time:** 13/12/2021 02:27:29
* **Coordinates:** Latitude 55.223067, Longitude 14.243730


### **The collision trajectory**
<p align="center">
  <img src="./real_collision_map.png" alt="Map of the real collision" width="80%"/>
</p>

*Interpretation:* The visualization shows Ship 2 (Red) striking Ship 1 (Blue) at the orange star point. At the point of impact, Ship 1's data completely stops/vanishes (it went dark), while Ship 2 continues forward in a bit of a "disoriented" way. This seems logical.

---

## **AI use disclosure**

Artificial intelligence (OpenAI GPT models) was utilized to brainstorm the physical characteristics of a vessel collision, which helped with writing the full "convergence & signal death" logic after multiple false-positive results (before, the identified final-output crashes were truly not clear crashes, when plotted). Also, helped to navigate the PySpark specifics and understand the commands needed. AI also assisted in troubleshooting Docker's virtual disk memory limitations when attempting to `COPY` the 16GB dataset into the image, leading to the optimized volume-mount architecture used in `docker-compose.yml`.

##

**Author:**
\
Ieva Vaškelytė, ieva.vaskelyte@mif.stud.vu.lt
