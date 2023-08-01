import requests
import json
import psycopg2

# Import credentials
with open("credentials.json", "r") as credentials_files:
    credentials = json.load(credentials_files)

# Extract data from source
url = "https://archive-api.open-meteo.com/v1/archive?latitude=-34.6131&longitude=-58.3772&start_date=2020-01-01&end_date=2023-07-25&daily=temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,sunrise,sunset,precipitation_sum,rain_sum,precipitation_hours,windspeed_10m_max,shortwave_radiation_sum,et0_fao_evapotranspiration&timezone=auto"
# payload = {}
# headers = {}

response = requests.request("GET", url)#, headers=headers, data=payload)

print(f'Reponse status: {response.status_code}')

if response.status_code == 200:
    json_object = response.json()

#Estructure data in a list
    data_list = [
        {'time': json_object['daily']['time']},
        {'temperature_2m_max': json_object['daily']['temperature_2m_max']},
        {'temperature_2m_min': json_object['daily']['temperature_2m_min']},
        {'apparent_temperature_max': json_object['daily']['apparent_temperature_max']},
        {'apparent_temperature_min': json_object['daily']['apparent_temperature_min']},
        {'sunrise': json_object['daily']['sunrise']},
        {'sunset': json_object['daily']['sunset']},
        {'precipitation_sum': json_object['daily']['precipitation_sum']},
        {'rain_sum': json_object['daily']['rain_sum']},
        {'precipitation_hours': json_object['daily']['precipitation_hours']},
        {'windspeed_10m_max': json_object['daily']['windspeed_10m_max']},
        {'shortwave_radiation_sum': json_object['daily']['shortwave_radiation_sum']},
        {'et0_fao_evapotranspiration': json_object['daily']['et0_fao_evapotranspiration']},
    ]
    print(f'Extraction successfull. Data structured in a list.')

else:
    print(f"Failed data extraction. Status code: {response.status_code}")  



#Establish the connection to Redshift
connection = psycopg2.connect(
    host=credentials.get('host'),
    user=credentials.get('user'),
    password=credentials.get('password'),
    dbname=credentials.get('database_name'),
    port=credentials.get('port'),
)

# Establish Table name
table_name = 'joseandresmelendezc_coderhouse.clima_ba'

# Create Table
try:
    with connection.cursor() as cursor:
        create_table_query = f'''
        CREATE TABLE {table_name} (
            time time,
            temperature_2m_max numeric,
            temperature_2m_min numeric,
            apparent_temperature_max numeric,
            apparent_temperature_min numeric,
            sunrise time,
            sunset time,
            precipitation_sum numeric,
            rain_sum numeric,
            precipitation_hours numeric,
            windspeed_10m_max numeric,
            shortwave_radiation_sum numeric,
            et0_fao_evapotranspiration numeric
        );'''

        cursor.execute(create_table_query)

# Commit the changes to the database
    connection.commit()
    print("Table created successfully!")

except Exception as e:
# Handle any errors
    print("Error: ", e)





 