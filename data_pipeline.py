import requests
from datetime import datetime, timedelta
import pandas as pd
import time 
import sqlite3
from Config import API_KEY, FILE_PATH

sales_data = pd.read_csv(FILE_PATH)
 

# Step 2: Data Transformation (JSONPlaceholder API - /users)

def fetch_user_data():
    url = 'https://jsonplaceholder.typicode.com/users'
    response = requests.get(url)
    user_data = response.json()
    return user_data


def merge_user_data(sales_data, user_data):
    merged_data = []

    for index, sale in sales_data.iterrows():
        customer_id = sale['customer_id']
        matching_users = [user for user in user_data if user['id'] == customer_id]

        if matching_users:
            user = matching_users[0]
            sale = sale.tolist()
            merged_sale = sale + [user['name'], user['username'], user['email'], user['address']['city'], user['address']['geo']['lat'], user['address']['geo']['lng']]
            merged_data.append(merged_sale)

    return merged_data

# Fetch and merge user data
user_data = fetch_user_data()
merged_data = merge_user_data(sales_data, user_data)

 

# Step 3: Data Transformation (OpenWeatherMap API)

def add_weather_data(merged_data):
    data_with_weather = []

    for row in merged_data:
        lat = row[10]  # Assuming latitude is at index 10 in the merged data
        lon = row[11]  # Assuming longitude is at index 11 in the merged data

        # Fetch weather data for the latitude and longitude using a weather API
        weather_data = fetch_weather_data(lat, lon)

        # Access temperature and weather_condition using the get() method with default values
        temperature = weather_data.get('main', {}).get('temp', None)
        weather_condition = weather_data.get('weather', [{}])[0].get('description', None)

        # Add the weather data to the row
        row.append(temperature)
        row.append(weather_condition)

        data_with_weather.append(row)

    return data_with_weather

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

def fetch_weather_data(lat, lon):

    url = f'http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}'
    response = requests.get(url)
    time.sleep(1)
    return response.json()


# Add weather data to merged sales data
data_with_weather = add_weather_data(merged_data)


# Step 4: Data Manipulation and Aggregations

def perform_data_aggregations(data_with_weather):
    # Convert the list to a DataFrame
    data_df = pd.DataFrame(data_with_weather, columns=['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'order_date', 'name', 'username', 'email', 'city', 'latitude', 'longitude', 'temperature', 'weather_condition' ])
    
    # Convert the 'order_date' column to datetime type
    data_df['order_date'] = pd.to_datetime(data_df['order_date'])

    # Convert the 'quantity' and 'price' columns to numeric type
    data_df['quantity'] = pd.to_numeric(data_df['quantity'])
    data_df['price'] = pd.to_numeric(data_df['price'])

    # Calculate total sales amount per customer
    customer_sales = (data_df['quantity'] * data_df['price']).groupby(data_df['customer_id']).sum()

    # Determine the average order quantity per product
    product_quantities = data_df.groupby('product_id')['quantity'].mean()

    # Identify the top-selling products
    top_selling_products = data_df.groupby('product_id')['quantity'].sum().nlargest(10)

    # Identify the top customers
    top_customers = customer_sales.nlargest(5)

    # Analyze sales trends over time
    sales_trends = data_df.groupby(data_df['order_date'].dt.to_period('M'))['quantity'].sum()

    return {
        'customer_sales': customer_sales,
        'product_quantities': product_quantities,
        'top_selling_products': top_selling_products,
        'top_customers': top_customers,
        'sales_trends': sales_trends
    }

# Perform data manipulations and aggregations
aggregated_data = perform_data_aggregations(data_with_weather)

print("Data manipulations and aggregations performed.")

# Step 5: Data Storage - Store the transformed data in a database



def store_data_in_database(aggregated_data, sales_trends, data_with_weather, database_name):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    # Create tables
    create_tables(cursor)

    
    #if there is data in the tables from previous runs, delete it.
    #cursor.execute(f"DELETE FROM CustomerSales")
    #cursor.execute(f"DELETE FROM SalesDataTransformed")
    #cursor.execute(f"DELETE FROM SalesTrends")
    #cursor.execute(f"DELETE FROM ProductQuantity")
    #cursor.execute(f"DELETE FROM TopSellingProducts")
     


    # Insert data into tables
    insert_data(cursor, aggregated_data)

    #Insert Product Quantiy data into a table
    store_product_quantities(cursor,aggregated_data)


    #Insert Top Selling Products data into a table
    store_top_selling_products(cursor,aggregated_data)

    # Store sales trends in a separate table
    store_sales_trends(cursor, sales_trends)

    # Insert Sales Transformed data 
    insert_transformed_data(cursor, data_with_weather)

    # Commit and close connection
    conn.commit()
    conn.close()



def create_tables(cursor):

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CustomerSales (
            customer_id INTEGER PRIMARY KEY,
            total_sales_amount REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS SalesTrends (
            month TEXT PRIMARY KEY,
            total_quantity INTEGER
        )
    """)


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ProductQuantity (
            Product_id INTEGER PRIMARY KEY,
            AverageQuantity REAL
        )
    """)


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS TopSellingProducts (
            Product_id INTEGER PRIMARY KEY,
            TotalQuantity REAL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS SalesDataTransformed (
            order_id INTEGER,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price REAL,
            order_date DATE,
            name TEXT,
            username TEXT,
            email TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            temperature REAL,
            weather TEXT
        )
    """)

 

def insert_data(cursor, aggregated_data):
    # Insert customer sales data
    for customer_id, total_sales_amount in aggregated_data['customer_sales'].items():
        cursor.execute("INSERT INTO CustomerSales VALUES (?, ?)", (customer_id, total_sales_amount))


def store_sales_trends(cursor, sales_trends):
    # Insert sales trends data
    for month, total_quantity in sales_trends.items():
        cursor.execute("INSERT INTO SalesTrends VALUES (?, ?)", (str(month), total_quantity))


def store_top_selling_products(cursor,aggregated_data):
     #Insert  Total Quantity by Product Data
    for product_id, quantity in aggregated_data['top_selling_products'].items(): 
        cursor.execute("INSERT INTO TopSellingProducts VALUES (?, ?)",(product_id, quantity))


def store_product_quantities(cursor,product_quantities ):
    #Insert Produc Quantity Data
    for product_id, quantity in aggregated_data['product_quantities'].items(): 
        cursor.execute("INSERT INTO ProductQuantity VALUES (?, ?)",(product_id, quantity))


def insert_transformed_data(cursor, data_with_weather):
     
    for order_id, customer_id, product_id, quantity, price, order_date, name, username, email, address_city, address_geo_lat, address_geo_lng, temperature, weather in data_with_weather:
 
        # Insert the data into the table
        cursor.execute(""" INSERT INTO SalesDataTransformed (order_id, customer_id, product_id, quantity, price, order_date, name, username, email, city, latitude, longitude, temperature, weather
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """, (order_id, customer_id, product_id, quantity, price, order_date, name, username, email, address_city, address_geo_lat, address_geo_lng, temperature, weather))



# Specify the database name
database_name = 'sales_data.db'

# Store the transformed and aggregated data in the database
store_data_in_database(aggregated_data, aggregated_data['sales_trends'], data_with_weather, database_name)

print("Data stored in the database.")
