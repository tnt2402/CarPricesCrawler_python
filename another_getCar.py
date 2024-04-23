import csv
import json
import os
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


def process_vin(tmp_vins):
    try:
        url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/';

        post_fields = {'format': 'json', 'data': tmp_vins}
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
all_vin_results = []
for i in tqdm(range(0, len(vin_list), n)):
# for i in tqdm(range(2)):
    tmp_vins = '; '.join(tmp_vin['vin'] for tmp_vin in vin_list[i:i+n])
    # print(tmp_vins + '\n')
    result = process_vin(tmp_vins)  
    all_vin_results.extend((result['Results']))
# print(all_vin_results)


# os.exit(0)
    
all_car = []
if len(all_vin_results) == len(vin_list):
    for i, each_car in enumerate(vin_list):
        tmp_car = each_car
        tmp_car['displacement_l'] = all_vin_results[i]['DisplacementL']
        tmp_car['drive_type'] = all_vin_results[i]['DriveType']
        tmp_car['engine_model'] = all_vin_results[i]['EngineCylinders']
        tmp_car['fuel_type_primary'] = all_vin_results[i]['FuelTypePrimary']
        tmp_car['fuel_type_secondary'] = all_vin_results[i]['FuelTypeSecondary']
        tmp_car['doors'] = all_vin_results[i]['Doors']
        tmp_car['seat_rows'] = all_vin_results[i]['SeatRows']
        tmp_car['seats'] = all_vin_results[i]['Seats']
        
        all_car.append(tmp_car)

write_to_csv(all_car, "merged_data_with_vin_data.csv")
write_to_csv(all_vin_results, "vin_data.csv")