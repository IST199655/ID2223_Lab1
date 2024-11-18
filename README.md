# Lab 1 - Air Quality Prediction Service

The goal of this lab work is to setup a Serverless ML System that Predicts Air Quality for a Location, and was develped in the context of course ID2223 at KTH. The functioning system is made of 4 jupyter notebook files, which interact with Hopsworks Serverless store and github to create a dashboard where air quality predictions can be seen of the next 7 days and every previous prediction is compared to ground truth in order to check on the model's previous performance.

The four notebooks have the following purposes:

### 1.Backfill_features.ipynb

In this notebook, the labels needed to train the model are backfilled from historical data downloaded from [aqicn.org](https://aqicn.info) saved in a .csv within the data foulder. The features were downloaded using Open_Meteo API. The station chosen is located in Råsundavägen 107, Solna, Sweden and data backfill was downloaded on the 16th of November 2024. This notebook is run manually only once when starting up the system, creating the hopsworks feature stores used by the rest of the system.

### 2.Update_daily.ipynb

This notebook is run daily by Github Actions to update the feature stores containing the features and labels, obtaining weather forecast which will be used to predict air quality in the next 7 days. The workflow which performs this action is contained in /.github/workflows/update_daily_weather.yml file.

### 3.Training_pipeline.ipynb

This notebook uses the features and labels in the feature stores to train a XGBoost model to predict air quality based on weather forecast features. This model is then saved to Hopsworks model registry and air_quality_model directory so it can be used for batch inference. In this notebook the Mean Squared Error, the R squared and the feature importances of the model are also evaluated.

### 4.Batch_inference.ipynb

This notebook is run daily by Github Actions to perform batch inference based on the updated feature store data (by 2. notebook), update dashboard in github and evaluate previous predictions, saving them in a feature store.

## Access to dashboard

Batch inference Dashboard can be accessed at:

[Hindcast](https://id2223lab1-3wzmaajxejhjferptty8gw.streamlit.app/)

## Requirements

Within /.github/workflows/requirements.txt