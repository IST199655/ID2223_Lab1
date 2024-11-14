from datetime import datetime, timedelta
import requests
import pandas as pd
import hopsworks
import os
import warnings
from functions import util


## GET THE AIR QUALITY FORECAST FOR THE NEXT DAYS

# Here hopsworks key is not a secret 

# with open('../../data/hopsworks-api-key.txt', 'r') as file:
#     os.environ["HOPSWORKS_API_KEY"] = file.read().rstrip()

## Here it is

# Get the API key from GitHub Secrets
HOPSWORKS_API_KEY = os.getenv('HOPSWORKS_API_KEY')

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


## GET THE WEATHER FORECAST FOR THE NEXT DAYS 

# Coordinates of Stockholm
latitude = 59.3293
longitude = 18.0686

# Get the current date and calculate the forecast date range (e.g., next 7 days)
today = datetime.today().date()
forecast_start_date = today - timedelta(days=1)
forecast_end_date = today + timedelta(days=5)  # 7 days from today

df_forecast_weather = util.get_hourly_weather_forecast(city, latitude, longitude)

df_forecast_weather['date'] = df_forecast_weather['date'].dt.date

df_forecast_weather['date'] = pd.to_datetime(df_forecast_weather['date'])

# Group by the new 'day' column and calculate the daily average for each feature
df_forecast_weather = df_forecast_weather.groupby('date').agg({
    'temperature_2m_mean': 'mean',
    'precipitation_sum': 'mean',
    'wind_speed_10m_max': 'mean',
    'wind_direction_10m_dominant': 'mean'
}).reset_index()

df_forecast_weather = df_forecast_weather.drop(index=[9,8,7])



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