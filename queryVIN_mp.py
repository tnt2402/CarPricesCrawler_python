import csv
import json
from multiprocessing import Pool, cpu_count, Manager, freeze_support
import requests
from joblib import Parallel, delayed
from vpic import TypedClient
from pathlib import Path

c = TypedClient()

def write_to_csv(data, filename):
    keys = data[0].keys() if data else []
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

def process_vin(tmp_vin, results):
    try:
        url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/'

        post_fields = {'format': 'json', 'data': tmp_vin['vin']}
        r = requests.post(url, data=post_fields)

        data = json.loads(r.text)
        return data

    except:
        print(r)

# Open the CSV file
with open('merged_data.csv', 'r') as file:
    reader = csv.DictReader(file)

    # Create a list to store the vin attributes
    vin_list = []

    # Iterate over each row in the CSV file
    for row in reader:
        # Check if the row has a "vin" attribute
        if 'vin' in row:
            # Append the vin attribute to the list
            tmp_vin = {
                'vin': row['vin'],
                'name': row['name'],
                'model_id': row['model_id'],
                'make_name': row['make_name'],
                'model_name': row['model_name'],
                'body_style': row['body_style'],
                'price': row['price'],
                'year': row['year'],
                'normalized_color_exterior': row['normalized_color_exterior'],
                'normalized_color_interior': row['normalized_color_interior'],
                'transmission': row['transmission'],
                'trim': row['trim'],
                'engine_cylinders': row['engine_cylinders']
            }
            vin_list.append(tmp_vin)

# Process VINs in parallel
n = 50


all_car = []
for i, each_car in enumerate(vin_list):
    tmp_car = each_car
    tmp_car['displacement_l'] = results[i]['DisplacementL']
    tmp_car['drive_type'] = results[i]['DriveType']
    tmp_car['engine_model'] = results[i]['EngineModel']
    tmp_car['fuel_type_primary'] = results[i]['DisplacementL']
    tmp_car['doors'] = results[i]['Doors']
    all_car.append(tmp_car)

print(all_car)
write_to_csv(all_car, Path("merged_data_with_vin_data.csv"))