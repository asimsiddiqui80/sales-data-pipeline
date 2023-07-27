import pandas as pd
import requests
import time
from Config import API_KEY, FILE_PATH


# Step 1: Load the generated sales data from CSV
sales_data = pd.read_csv(FILE_PATH)


# Step 2: Fetch user data from JSONPlaceholder API
users_response = requests.get('https://jsonplaceholder.typicode.com/users')
print(users_response.json())
users_data = pd.json_normalize(users_response.json())[['id', 'name', 'username', 'email', 'address.city', 'address.geo.lat', 'address.geo.lng']]

# Merge sales data with user data based on customer_id
sales_data = pd.merge(sales_data, users_data, left_on='customer_id', right_on='id', how='left')

# Step 3: Fetch weather data from OpenWeatherMap API
def fetch_weather_data(latitude, longitude):
    url = f'http://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={API_KEY}'
    response = requests.get(url)
    time.sleep(1)
    return response.json()

def mock_fetch_weather_data(latitude, longitude):
    return  {
  "coord": {
    "lon": longitude,
    "lat": latitude
  },
  "weather": [
    {
      "id": 501,
      "main": "Rain",
      "description": "moderate rain",
      "icon": "10d"
    }
  ],
  "base": "stations",
  "main": {
    "temp": 298.48,
    "feels_like": 298.74,
    "temp_min": 297.56,
    "temp_max": 300.05,
    "pressure": 1015,
    "humidity": 64,
    "sea_level": 1015,
    "grnd_level": 933
  },
  "visibility": 10000,
  "wind": {
    "speed": 0.62,
    "deg": 349,
    "gust": 1.18
  },
  "rain": {
    "1h": 3.16
  },
  "clouds": {
    "all": 100
  },
  "dt": 1661870592,
  "sys": {
    "type": 2,
    "id": 2075663,
    "country": "IT",
    "sunrise": 1661834187,
    "sunset": 1661882248
  },
  "timezone": 7200,
  "id": 3163858,
  "name": "Zocca",
  "cod": 200
}
                             

                           
print(fetch_weather_data(51.5074, 0.1278))
# Example: Assuming latitude and longitude columns exist in sales_data
sales_data['weather'] = sales_data.apply(lambda row: mock_fetch_weather_data(row['address.geo.lat'], row['address.geo.lng']), axis=1)

# Step 4: Store the transformed data in the database
# Assuming you have established a database connection, create tables and insert the data using your preferred method (e.g., SQLAlchemy, psycopg2, pymysql)

# Print the transformed data
sales_data.to_csv('sales_data_transformed.csv', index=False)
print(sales_data)

# Additional steps: Save the transformed data to a new CSV file or directly insert it into the database tables.

