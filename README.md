# Lab 1 - Air Quality Prediction Service

The goal of this lab work is to setup a Serverless ML System that Predicts Air Quality for a Location, and was develped in the context of course ID2223 at KTH. The functioning system is made of 4 jupyter notebook files, which interact with Hopsworks Serverless store and github to create a dashboard where air quality predictions can be seen of the next 7 days and every previous prediction is compared to ground truth in order to check on the model's previous performance.

The four notebooks have the following purposes:

### 1.Bakcfill_features.ipynb

In this notebook, the labels needed to train the model are backfilled from historical data downloaded from [aqicn.org](https://aqicn.info) saved in a .csv within the data foulder. The features were downloaded using Open_Meteo API. The station chosen is located in Råsundavägen 107, Solna, Sweden and data backfill was downloaded on the 16th of November 2024. This notebook is run manually only once when starting up the system, creating the hopsworks feature stores used by the rest of the system.

### 2.Update_daily.ipynb

