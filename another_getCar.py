import csv
import json
import requests
from tqdm import tqdm
from joblib import Parallel, delayed
from vpic import TypedClient
c = TypedClient()


def write_to_csv(data, filename):
    keys = data[0].keys() if data else []
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


def process_vin(each_vin):
    url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/';

    post_fields = {'format': 'json', 'data': each_vin['vin']}
    print(each_vin['vin'])
    r = requests.post(url, data=post_fields)
    data = json.loads(r.text)
    tmp_car = each_vin
    tmp_car['displacement_l'] = data['Results'][0]['DisplacementL']
    tmp_car['drive_type'] = data['Results'][0]['DriveType']
    tmp_car['engine_model'] = data['Results'][0]['EngineModel']
    tmp_car['fuel_type_primary'] = data['Results'][0]['FuelTypePrimary']
    tmp_car['doors'] = data['Results'][0]['Doors']
    return tmp_car


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

for i in 

write_to_csv(results, "merged_data_with_vin_data.csv")