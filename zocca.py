#%% Step 1: Fetch Data from API

import requests
import pandas as pd

# Define the API URL (replace {API key} with your actual API key)
api_url = "https://api.openweathermap.org/data/2.5/weather?lat=44.34&lon=10.99&appid=fc284335"

# Function to fetch and display weather data
def fetch_weather_data(url):
    try:
        # Send a GET request to the API
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the JSON response
        weather_data = response.json()

        # Print the raw JSON response to view its structure
        print("Raw JSON Response:")
        print(weather_data)

        # Optional: You can pretty-print the JSON for better readability
        import json
        print("\nFormatted JSON Response:")
        print(json.dumps(weather_data, indent=4))

        return weather_data

    except requests.exceptions.RequestException as e:
        print("An error occurred while accessing the API:", e)
        return None

# Call the function to fetch the data
weather_data = fetch_weather_data(api_url)

#%% Step 2: Transform Data to DataFrame

if weather_data:
    # Extract relevant data from the response
    data = {
        "Longitude": weather_data["coord"]["lon"],
        "Latitude": weather_data["coord"]["lat"],
        "Weather": weather_data["weather"][0]["main"],
        "Description": weather_data["weather"][0]["description"],
        "Temperature": weather_data["main"]["temp"],
        "Feels_Like": weather_data["main"]["feels_like"],
        "Temp_Min": weather_data["main"]["temp_min"],
        "Temp_Max": weather_data["main"]["temp_max"],
        "Pressure": weather_data["main"]["pressure"],
        "Humidity": weather_data["main"]["humidity"],
        "Visibility": weather_data["visibility"],
        "Wind_Speed": weather_data["wind"]["speed"],
        "Wind_Direction": weather_data["wind"]["deg"],
        "Cloudiness": weather_data["clouds"]["all"],
        "Sunrise": weather_data["sys"]["sunrise"],
        "Sunset": weather_data["sys"]["sunset"],
        "Timezone": weather_data["timezone"],
        "City_ID": weather_data["id"],
        "City_Name": weather_data["name"],
        "Code": weather_data["cod"]
    }

    # Convert the extracted data into a DataFrame
    df = pd.DataFrame([data])

    # Display the DataFrame
    print("\nWeather Data DataFrame:")
    print(df)

    # Save the DataFrame to a CSV file
    df.to_csv('weather_data.csv', index=False)
    print('Data saved to "weather_data.csv".')
else:
    print("No data to transform to DataFrame.")

#%% Step 3: Load Data into a Database

# Import necessary libraries for database operations
import pyodbc
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Define the database connection string for connecting to SQL Server
DATABASE_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ARMSTRONG;"
    "Database=weather;"
    "Trusted_Connection=yes;"
)

# Set up logging to track script activity
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Add console logging for real-time feedback
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# Log the start of the script
logging.info("Script started.")

def upload_data(table, dataframe, upload_type):
    """
    Upload data to a specified table in the database.

    Parameters:
        table (str): Name of the table to upload data.
        dataframe (DataFrame): Pandas DataFrame containing data to upload.
        upload_type (str): Method of upload ('replace', 'append', etc.).

    Returns:
        None
    """
    try:
        logging.info("Attempting to connect to the database for uploading data.")
        # Create an SQLAlchemy engine for database connection
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={DATABASE_CONNECTION_STRING}")
        # Upload the DataFrame to the database table
        logging.info(f"Uploading data to table: {table}")
        dataframe.to_sql(table, engine, index=False, if_exists=upload_type, schema="dbo", chunksize=10000)
        logging.info(f"Data uploaded successfully to {table}.")
    except Exception as e:
        # Log any errors that occur during the upload process
        logging.error(f"Error uploading data: {e}")
        print(f"Error uploading data: {e}")


# Specify the table name and upload type
table_name = "zocco"
upload_type = "append"  # Options: 'replace', 'append'

# Upload the transformed data to the database
try:
    upload_data(table_name, df, upload_type)
    logging.info("Data uploaded successfully.")
    print("Data uploaded successfully.")
except Exception as e:
    logging.error(f"Failed to upload data: {e}")
    print(f"Failed to upload data: {e}")

# Log the end of the script
logging.info("Script ended.")

# %%
