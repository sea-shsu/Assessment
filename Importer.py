import os
import zipfile
import csv
import sqlite3
import sys
from datetime import datetime
import pytz



# Create a new folder for the database
if not os.path.exists('database'):
    os.mkdir('database')

# Connect to SQLite database in the new folder
conn = sqlite3.connect('database/test_project.db')
cursor = conn.cursor()

# Create load_data table
cursor.execute('''
    CREATE TABLE load_data (
        OperDay DATE,
        HourEnding HOUR,
        COAST INTEGER,
        EAST INTEGER,
        FAR_WEST INTEGER,
        NORTH INTEGER,
        NORTH_C INTEGER,
        SOUTHERN INTEGER,
        SOUTH_C INTEGER,
        WEST INTEGER,
        TOTAL INTEGER,
        DSTFlag TEXT
    )
''')

# Create separate tables for each region
regions = ["KDAL", "KHOU", "KSAT"]
for region in regions:
    cursor.execute(f'''
    CREATE TABLE {region} (
        TimeCST TEXT,
        TemperatureF INTEGER,
        DewPointF INTEGER,
        Humidity INTEGER,
        Sea_Level_PressureIn INTEGER,
        VisibilityMPH INTEGER,
        Wind_Direction TEXT,
        Wind_SpeedMPH INTEGER,
        Gust_SpeedMPH INTEGER,
        PrecipitationIn TEXT,
        Events TEXT,
        Conditions TEXT,
        WindDirDegrees INTEGER,
        DateUTC DATE

    )
    ''')

# Load data into load_data table
load_data_folder = r'C:\Users\m_abd\Downloads\saracen-energy-coding-assessment\saracen-project\system_load_by_weather_zone'
# Load data into load_data table
for file_name in os.listdir(load_data_folder):
    if file_name.endswith(".zip"):
        with zipfile.ZipFile(os.path.join(load_data_folder, file_name), "r") as zip_ref:
            for csv_file_name in zip_ref.namelist():
                if csv_file_name.endswith(".csv"):
                    with zip_ref.open(csv_file_name, "r") as csv_file:
                        # Read CSV file into memory and skip header row
                        csv_data = csv_file.read().decode("utf-8").splitlines()[1:]

                        # Write each row to load_data table
                        for row in csv_data:
                            # Convert date string to datetime object
                            date_string = row.split(",")[0]
                            datetime_object = datetime.strptime(date_string, '%m/%d/%Y')

                            # Parse the hour range string and calculate the corresponding hour value
                            hour_string = row.split(",")[1]
                            hour, minute = hour_string.split(":")
                            hour_int = int(hour)

                            cursor.execute('''
                            INSERT INTO load_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                datetime_object.date(),
                                hour_int,
                                float(row.split(",")[2]),
                                float(row.split(",")[3]),
                                float(row.split(",")[4]),
                                float(row.split(",")[5]),
                                float(row.split(",")[6]),
                                float(row.split(",")[7]),
                                float(row.split(",")[8]),
                                float(row.split(",")[9]),
                                float(row.split(",")[10]),
                                row.split(",")[11]
                            ))
conn.commit()
# Insert rows

def check_float(value):
    try:
        return float(value)
    except ValueError:
        return None

def check_direction(direction):
    if direction.lower() == 'calm':
        return None
    return direction


# Load data into weather_data tables
weather_data_folder = r'C:\Users\m_abd\Downloads\saracen-energy-coding-assessment\saracen-project\weather_data'
# Loop through each zip file in the folder
for zip_file_name in os.listdir(weather_data_folder):
    if zip_file_name.endswith(".zip"):
        region = zip_file_name.split("_")[0]

        # Loop through each CSV file in the zip file
        with zipfile.ZipFile(os.path.join(weather_data_folder, zip_file_name), "r") as zip_ref:
            for csv_file_name in zip_ref.namelist():
                # Read CSV file into memory and skip header row
                csv_data = zip_ref.read(csv_file_name).decode("utf-8").splitlines()[1:]

                # Insert each row into the appropriate region's table
                for row in csv_data:
                    date_string = row.split(",")[13]  # Extract date only
                    #datetime_object2 = datetime.strptime(date_string, '%Y-%m-%d')
                    datetime_object2 = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

                    target_timezone = pytz.timezone('US/Central')

                    # convert the datetime object to the target timezone
                    datetime_object2 = datetime_object2.astimezone(target_timezone)

                    # Check if the value is not 'N/A'
                    humidity = row.split(",")[3]
                    if humidity != 'N/A' :
                        humidity = int(humidity)
                    else:
                        humidity = None

                    wind_speed = check_float(row.split(",")[7])
                    wind_direction = check_direction(row.split(",")[6])
                    gust_speed = check_float(row.split(",")[8])
                    time = row.split(",")[0]
                    temp = float(row.split(",")[1])
                    dew_pt = float(row.split(",")[2])
                    sea_lvl_press = float(row.split(",")[4])
                    visMPH = float(row.split(",")[5])

                    sql = f'''
                                    INSERT INTO {region} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    '''
                    params = (
                        time,
                        temp,
                        dew_pt,
                        humidity,
                        sea_lvl_press,
                        visMPH,
                        wind_direction,
                        wind_speed,
                        gust_speed,
                        row.split(",")[9],
                        row.split(",")[10],
                        row.split(",")[11],
                        int(row.split(",")[12]),
                        datetime_object2.date()
                    )
                    try:
                        cursor.execute(sql, params)
                    except Exception as e:
                        print(e)
# Commit changes and close connection
conn.commit()
