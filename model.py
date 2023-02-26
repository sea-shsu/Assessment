import sqlite3
import pytz
import datetime

def get_load_data(db_path, location):
    conn = sqlite3.connect('database/test_project.db')
    c = conn.cursor()

    # query for 2014 load data for given location aggregated by week
    query_2014 = f"SELECT strftime('%Y-%W', OperDay) AS week, AVG({location}) FROM load_data WHERE strftime('%Y', OperDay) = '2014' GROUP BY week"

    # query for 2015 load data for given location aggregated by week
    query_2015 = f"SELECT strftime('%Y-%W', OperDay) AS week, AVG({location}) FROM load_data WHERE strftime('%Y', OperDay) = '2015' GROUP BY week"

    c.execute(query_2014)
    data_2014 = c.fetchall()

    c.execute(query_2015)
    data_2015 = c.fetchall()

    conn.close()

    return data_2014, data_2015

def get_weather_data(db_path, weather_region):
    conn = sqlite3.connect('database/test_project.db')
    c = conn.cursor()

    # query weather data for 2014
    query_2014 = f"SELECT strftime('%Y-%W', DateUTC) AS week, AVG(TemperatureF), AVG(DewPointF), AVG(Humidity), AVG([Sea_Level_PressureIn]), " \
                 f"AVG(VisibilityMPH), AVG([Wind_SpeedMPH]), AVG([Gust_SpeedMPH]) FROM {weather_region} WHERE strftime('%Y', DateUTC) = '2014' GROUP BY week"
    c.execute(query_2014)
    weather_data_2014 = c.fetchall()

    # query weather data for 2015
    query_2015 = f"SELECT strftime('%Y-%W', DateUTC) AS week, AVG(TemperatureF), AVG(DewPointF), AVG(Humidity), AVG([Sea_Level_PressureIn]), " \
                 f"AVG(VisibilityMPH), AVG([Wind_SpeedMPH]), AVG([Gust_SpeedMPH]) FROM {weather_region} WHERE strftime('%Y', DateUTC) = '2015' GROUP BY week"
    c.execute(query_2015)
    weather_data_2015 = c.fetchall()

    conn.close()

    return weather_data_2014, weather_data_2015

load_data_2014, load_data_2015 = get_load_data('test_project.db', 'EAST')
weather_data_2014, weather_data_2015 = get_weather_data('test_project.db', 'KHOU')
