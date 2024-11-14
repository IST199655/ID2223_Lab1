from datetime import datetime, timedelta
import requests
import pandas as pd
import hopsworks
import os
import warnings
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


## GET THE AIR QUALITY FORECAST FOR THE NEXT DAYS

# Here hopsworks key is not a secret 

with open('../../data/hopsworks-api-key.txt', 'r') as file:
     os.environ["HOPSWORKS_API_KEY"] = file.read().rstrip()

## Here it is

# Get the API key from GitHub Secrets
#HOPSWORKS_API_KEY = os.getenv('HOPSWORKS_API_KEY')

#Get AQI API KEY from secrets of hopsworks

proj = hopsworks.login(project="ID2223LAB1KTH")

conn = hopsworks.connection(host="c.app.hopsworks.ai", project=proj, api_key_value=os.environ.get('HOPSWORKS_API_KEY'))
secrets = conn.get_secrets_api()

AQI_API_KEY = secrets.get_secret("AQI_API_KEY").value


country="sweden"
city = "stockholm"
street = "stockholm-hornsgatan-108"
aqicn_url="https://api.waqi.info/feed/@10009"


# Fetch air quality data from the AQICN API
response = requests.get(f"{aqicn_url}/?token={AQI_API_KEY}")

# Check if the request was successful
if response.status_code == 200:
    data = response.json()

    # 5. Parse the data 
    pm25_data = data.get("data", {}).get("forecast", {}).get("daily", {}).get("pm25", [])
    pm10_data = data.get("data", {}).get("forecast", {}).get("daily", {}).get("pm10", [])


    pm25_df = pd.DataFrame(pm25_data)
    pm10_df = pd.DataFrame(pm10_data)

    pm25_df = pm25_df[['day', 'avg']].rename(columns={'avg': 'pm25'})
    pm10_df = pm10_df[['day', 'avg']].rename(columns={'avg': 'pm10'})
    

    df_forecast_aq = pm25_df.merge(pm10_df, on="day", how="inner").drop(index=0)
    df_forecast_aq[['pm25', 'pm10']] = df_forecast_aq[['pm25', 'pm10']].astype(int)
   
    df_forecast_aq['day'] = pd.to_datetime(df_forecast_aq['day'])
    df_forecast_aq.rename(columns={'day': 'date'}, inplace=True)


## GET THE WEATHER FORECAST FOR THE NEXT DAYS and yesterday


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

latitude = 59.3293
longitude = 18.0686

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": latitude,
	"longitude": longitude,
	"daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "wind_speed_10m_max", "wind_direction_10m_dominant"],
	"past_days": 1
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process daily data. The order of variables needs to be the same as requested.
daily = response.Daily()
daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
daily_precipitation_sum = daily.Variables(2).ValuesAsNumpy()
daily_wind_speed_10m_max = daily.Variables(3).ValuesAsNumpy()
daily_wind_direction_10m_dominant = daily.Variables(4).ValuesAsNumpy()

daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s"),
	end = pd.to_datetime(daily.TimeEnd(), unit = "s"),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}

daily_data["temperature_2m_max"] = daily_temperature_2m_max
daily_data["temperature_2m_min"] = daily_temperature_2m_min
daily_data["precipitation_sum"] = daily_precipitation_sum
daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

df_forecast_weather = pd.DataFrame(data = daily_data)


# # UPDATE THE FEATURE GROUPS

fs = proj.get_feature_store()

    # Define the Feature Group for Forecast Air Quality Data
feature_group_name = "stockholm_weather"
version = 1

fg = fs.get_feature_group(name=feature_group_name, version=version)

fg.insert(df_forecast_weather)


feature_group_name = "stockholm_air_quality"
version = 1

fg = fs.get_feature_group(name=feature_group_name, version=version)

fg.insert(df_forecast_aq)