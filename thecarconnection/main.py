import ssl
import requests
import json
import csv
import cloudscraper
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from datetime import datetime

def get_current_time_string():
    # Get the current time
    now = datetime.now()
    # Format the time as YYYYMMDD_HHMMSS
    time_string = now.strftime("%Y%m%d_%H%M%S")
    return time_string

def write_to_csv(data, filename):
    keys = data[0].keys() if data else []
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

def crawl_pages(url, make_brands):
    # bypass cloudflare
    scraper = cloudscraper.CloudScraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False
        },
        ssl_context=ssl._create_unverified_context()
    )
    
    results = []
    count_car = 0
    for brand in make_brands:
        try:
            print("[+] Brand: %s\n" % brand)
            page_url = url + str(brand)

            response = scraper.get(page_url, verify=False)
            
            # Parse the JSON response
            data = json.loads(response.text)
            list_of_cars = data['listings']
            print("    Number of cars: %s\n" % len(list_of_cars))
            if len(list_of_cars) > 0:
                for car in list_of_cars:
                    tmp_car = {}
                    tmp_car['vin'] = car['vin']
                    tmp_car['name'] = car['make'] + ' ' + car['model']
                    tmp_car['model_id'] = car['model_id']
                    tmp_car['make_name'] = car['make']
                    tmp_car['model_name'] = car['model']
                    tmp_car['body_style'] = car['body']
                    tmp_car['door_count'] = car['doors']
                    tmp_car['engine_cylinders'] = car['cylinders']
                    tmp_car['price'] = car['price']
                    tmp_car['transmission'] = car['transmission']
                    tmp_car['trim'] = car['trim']
                    tmp_car['year'] = car['year']
                    tmp_car['normalized_color_exterior'] = car['exterior_color']
                    tmp_car['normalized_color_interior'] = car['interior_color']

                    results.append(tmp_car)
                    count_car += 1
        except: 
            continue
    print("[+] Total cars: %d\n" % count_car)
    return results



# old car with new-flag=0
url = "https://www.thecarconnection.com/blocks/listings-ppc/retrieve?numListings=2000&numColumns=4&model=&zip=90245&range=&new-flag=&category=&categories=&year-low=&year-high=&price-low=&price-high=&noprice-flag=1&make="

make_brands = [
    'acura', 'alfa-romeo', 'audi', 'bmw', 'buick', 'cadillac', 'chevrolet', 'chrysler', 'dodge', 'fiat', 'ford', 'genesis', 'gmc', 'honda', 'hyundai', 'infiniti', 'jaguar', 'jeep', 'kia', 'land-rover', 'lexus', 'lincoln', 'lucid', 'mazda', 'mercedes-benz', 'mini', 'mitsubishi', 'nissan', 'polestar', 'porsche', 'ram', 'rivian', 'subaru', 'tesla', 'toyota', 'volkswagen', 'volvo'
]
# make_brands = ['acura']
all_results = crawl_pages(url, make_brands)

# new car with new-flag=0
url = "https://www.thecarconnection.com/blocks/listings-ppc/retrieve?numListings=2000&numColumns=4&model=&zip=90245&range=&new-flag=1&category=&categories=&year-low=&year-high=&price-low=&price-high=&noprice-flag=1&make="


all_results.extend(crawl_pages(url, make_brands))

write_to_csv(all_results, 'cars_thecarconnection_' + get_current_time_string() + '.csv')
