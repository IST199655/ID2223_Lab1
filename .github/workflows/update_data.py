import datetime
import requests
import pandas as pd
import hopsworks
import datetime
from pathlib import Path
from functions import util
import json
import re
import os
import warnings
warnings.filterwarnings("ignore")

aqi_api_key_file = '../../data/aqi-api-key.txt'

with open(aqi_api_key_file, 'r') as file:
    AQI_API_KEY = file.read().rstrip()

# TODO: Change these values to point to your Sensor
country="sweden"
city = "stockholm"
street = "stockholm-hornsgatan-108"
aqicn_url="https://api.waqi.info/feed/@10009"

