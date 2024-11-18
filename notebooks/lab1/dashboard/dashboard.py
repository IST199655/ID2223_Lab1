

import streamlit as st
from PIL import Image

# Title for the dashboard
st.title("Air Quality Forecast Dashboard (by Mathis DESERT and Diogo")

# Path to your local image file
image_path = "notebooks/lab1/dashboard/pm25_hindcast_5_days.png"  # Replace with your image file's name or path

# Open and display the image
try:
    image = Image.open(image_path)
    st.image(image, caption="Displayed Image", use_container_width=True)
except FileNotFoundError:
    st.error("Image file not found. Please check the file path.")
