import zipfile
import csv
import io

# working with the big 16GB file from AIS, reducing ir using filters
INPUT_ZIP = 'aisdk-2021-12.zip' 
OUTPUT_CSV = 'filtered_december_ais.csv'

# Geographic bounds to shrink file size
LAT_MIN, LAT_MAX = 54.0, 56.5
LON_MIN, LON_MAX = 13.0, 15.5

def is_valid_geography(row):
    try:
        lat = float(row.get('Latitude', ''))
        lon = float(row.get('Longitude', ''))
        return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX and row.get('MMSI')
    except (ValueError, TypeError):
        return False

def filter_ais_data():
    with zipfile.ZipFile(INPUT_ZIP, 'r') as archive:
        csv_files = [f for f in archive.namelist() if f.endswith('.csv')]
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as outfile:
            writer = None
            for file_name in csv_files:
                with archive.open(file_name) as f:
                    text_stream = io.TextIOWrapper(f, encoding='utf-8')
                    reader = csv.DictReader(text_stream)
                    if writer is None:
                        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                        writer.writeheader()
                    for row in reader:
                        if is_valid_geography(row):
                            writer.writerow(row)
    print("Pre-filtering complete.")

if __name__ == "__main__":
    filter_ais_data()
