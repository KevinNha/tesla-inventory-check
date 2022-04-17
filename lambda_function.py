import boto3
import json
import requests

from botocore.exceptions import ClientError
from database import Database
from time import sleep

QUERY_API = 'https://www.tesla.com/inventory/api/v1/inventory-results?query='

# query fields
SUBSCRIBED_CITIES = {
    "Vancouver": {
        'postal_code': 'V6T 1Z4',
        'region': 'BC',
        'MODEL': 'm3',
        'RANGE': 200,
        'MARKET': 'CA',
        'lng': -123.116226,
        'lat': 49.2606,
    },
    "Edmonton": {
        'postal_code': 'T5J 2C1',
        'region': 'AB',
        'MODEL': 'm3',
        'RANGE': 200,
        'MARKET': 'CA',
        'lng': -113.488731,
        'lat': 53.545055,
    },
    "Calgary": {
        'postal_code': 'T2P 1J9',
        'region': 'AB',
        'MODEL': 'm3',
        'RANGE': 200,
        'MARKET': 'CA',
        'lng': -114.062019,
        'lat': 51.044270,
    },
    "Montreal": {
        'postal_code': 'H4N 1L4',
        'region': 'QC',
        'MODEL': 'm3',
        'RANGE': 200,
        'MARKET': 'CA',
        'lng': -73.706540,
        'lat': 45.574030,
    },
    "Toronto": {
        'postal_code': 'M3M 3H8',
        'region': 'ON',
        'MODEL': 'm3',
        'RANGE': 200,
        'MARKET': 'CA',
        'lng': -79.493130,
        'lat': 43.742560,
    },
    "Ottawa": {
        'postal_code': 'K1N 9N4',
        'region': 'ON',
        'MODEL': 'm3',
        'RANGE': 200,
        'MARKET': 'CA',
        'lng': -75.695000,
        'lat': 45.424721,
    },
    
}

# result filters
RESULT_FILTERS = [
    "City",
    "INTERIOR",
    "IsDemo",
    "Price",
    "PAINT",
    "TRIM",
    "Year",
    "WHEELS",
    "TrimName",
]

# email fields
EMAIL_SENDER = "Tesla Inventory Bot <teslainventorybot@gmail.com>"
EMAIL_RECEIVERS = {
    'Vancouver': ['hyunjinnha@gmail.com'],
    'Montreal': ['m.gariepy16@gmail.com'],
    'Edmonton': ['kellenrosentreter@gmail.com'],
    'Calgary': ['m.gariepy16@gmail.com'],
    'Toronto': ['histevee@gmail.com', 'Sir.gonga@gmail.com', 'searaghi@gmail.com'],
    'Ottawa': ['Apexsoftware2019@gmail.com'],
    'Failure': ['hyunjinnha@gmail.com'],
}
CHARSET = 'UTF-8'

TABLE_NAME = 'tesla_model3_inventory_vancouver'

db = Database()
sesclient = boto3.client('ses')

def lambda_handler(event, context):
    for city in SUBSCRIBED_CITIES.keys():
        main_function(city)
        
def main_function(city):
    try:
        existing_cars_vins = process_db_vins(db.get_db_entries(TABLE_NAME, city, "vin"))

        new_car_data = get_car_data(city, "new")
        used_car_data = get_car_data(city, "used")
        car_inventory = new_car_data + used_car_data
        car_inventory_vins = set(car.get("vin") for car in car_inventory)
        
        remove_existing_vins_from_db(car_inventory_vins, existing_cars_vins)
        if len(car_inventory) == 0:
            print("No new cars at the moment.")
            return
        else:
            # remove from car vins that exist already
            unsold_cars_vins = existing_cars_vins.intersection(car_inventory_vins)
            extract_new_vehciles_only(car_inventory, unsold_cars_vins)
            if len(car_inventory) == 0:
                print("No new cars at the moment.")
                return
            print("new cars! " + str(len(car_inventory)))

            print("updating database...")
            update_database(city, car_inventory)

            print("sending email...")
            send_update_email(city)

            return {
                'statusCode': 200,
                'body': json.dumps('Lambda Function Complete!')
            }
    except Exception as e:
        print("Failed to run Lambda function")
        print(e.with_traceback(None))
        send_fail_email()
        return {
            'statusCode': 400,
        } 

def process_db_vins(db_entry_vins):
    existing_vins = []
    for db_vin in db_entry_vins:
        existing_vins.append(db_vin['vin']['S'])
    return set(existing_vins)

def get_car_data(city, condition): 
    query = build_query(city, condition)
    results = requests.get(QUERY_API + query).json()
    is_new = True if condition == "new" else False
    return process_results(results, is_new)

def build_query(city, condition):
    return json.dumps({
        "query": {
            "model": SUBSCRIBED_CITIES[city]["MODEL"],
            "condition": condition,
            "arrangeby": "Relevance",
            "order": "desc",
            "market": SUBSCRIBED_CITIES[city]["MARKET"],
            "zip": SUBSCRIBED_CITIES[city]["postal_code"],
            "range": SUBSCRIBED_CITIES[city]["RANGE"],
            "lat": SUBSCRIBED_CITIES[city]["lat"],
            "lng": SUBSCRIBED_CITIES[city]["lng"],
            "region": SUBSCRIBED_CITIES[city]["region"],
            "super_region": "north america",
            "lang": "en",
            "options": {

            }
        },
        "offset": 0,
        "count": 50,
        "outsideOffset": 0,
        "outsideSearch": False,
    })

def process_results(results, is_new) -> list[dict]:
    if results.get('total_matches_found') == 0:
        return []

    cars_results = results.get('results')

    cars = []
    for car in cars_results:
        car_data = organize_car_data(car, is_new)
        cars.append(car_data)

    return cars

def organize_car_data(car, is_new):
    data = {}
    for filter in RESULT_FILTERS:
        data.setdefault(filter, car.get(filter))
    data.setdefault("vin", car["VIN"])
    data.setdefault("isNew", is_new)

    return data

def remove_existing_vins_from_db(car_inventory_vins, existing_cars_vins):
    sold_cars_vins = existing_cars_vins - car_inventory_vins
    for vin in sold_cars_vins:
        existing_cars_vins.discard(vin)
        db.delete_from_db(TABLE_NAME, Type='S', Item_Key="vin", Value=vin)

def extract_new_vehciles_only(car_inventory, unsold_cars_vins):
    cars_to_remove = []
    for car in car_inventory:
        if car["vin"] in unsold_cars_vins:
            cars_to_remove.append(car)
    
    for car in cars_to_remove:
        car_inventory.remove(car)

def update_database(city, new_car_inventory: list[dict]):
    for car in new_car_inventory:
        Item={
            "vin": {
                "S": car.get("vin")
            },
            "year": {
                "S": str(car.get("Year"))
            },
            "name": {
                "S": car.get("TrimName")
            },
            "price": {
                "S": str(car.get("Price"))
            },
            "paint": {
                "S": car.get("PAINT")[0]
            },
            "interior_color": {
                "S": car.get("INTERIOR")[0]
            },
            "trim": {
                "S": car.get("TRIM")[0]
            },
            "wheels":{
                "S": car.get("WHEELS")[0]
            },
            "city": {
                "S": city
            },
            "city_granular": {
                "S": car.get("City")
            },
            "is_new": {
                "BOOL": car.get("isNew")
            },
            "is_demo": {
                "BOOL": car.get("IsDemo")
            },
        }
        db.update_database(TABLE_NAME, Item)

def send_update_email(city):
    SUBJECT_HEADER = f"New Tesla Inventory Updated For {city}!"
    BODY_TEXT = build_body_text(city)
    
    subject = {
        "Data": SUBJECT_HEADER,
        "Charset": CHARSET,
    }
    body = {
        "Text": {
            "Charset": CHARSET,
            "Data": BODY_TEXT,
        },
    }
    
    try: 
        response = sesclient.send_email(
            Destination={
                'ToAddresses': EMAIL_RECEIVERS[city],  
            },
            Message={
                "Body": body,
                "Subject": subject,
            },
            Source=EMAIL_SENDER,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email successfully sent")

def send_fail_email():
    '''
    Sends a lambda fail email only to the service owner.
    '''
    subject = {
        "Data": "Failed to run check-tesla-inventory lambda",
        "Charset": CHARSET,
    }
    body = {
        "Text": {
            "Charset": CHARSET,
            "Data": "",
        },
    }

    try: 
        response = sesclient.send_email(
            Destination={
                'ToAddresses': EMAIL_RECEIVERS['Failure'],  
            },
            Message={
                "Body": body,
                "Subject": subject,
            },
            Source=EMAIL_SENDER,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email successfully sent")

def build_body_text(city):
    BODY_TEXT = "New Tesla has been updated for your subscription. Here is the existing inventory."
    cars_in_db = db.get_db_entries(TableName=TABLE_NAME, city=city)
    for car in cars_in_db:
        message = (
            f'\n\n{car.get("year").get("S")} {car.get("trim").get("S")}'
            f'\nWheels: {car.get("wheels").get("S")}'
            f'\nExterior: {car.get("paint").get("S")}'
            f'\nInterior: {car.get("interior_color").get("S")}'
            f'\nPrice: {car.get("price").get("S")}'
            f'\nWas this car a demo car?: {car.get("is_demo").get("BOOL")}'
            f'\nIs this car new? {car.get("is_new").get("BOOL")}'
            f'\nVin: {car.get("vin").get("S")}'
            f'\nCity: {car.get("city_granular").get("S")}'
        )
        BODY_TEXT += message
    return BODY_TEXT

lambda_handler(0, 0)