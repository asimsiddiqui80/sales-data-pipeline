Setting up and running the data pipeline involves the following steps:

Prerequisite: Github account to download the code.

Step 1: Install Required Dependencies
Make sure to have Python and the necessary libraries installed. You can install the required libraries using pip:

'pip install pandas requests'


Step 2: Update the Config File
Update Config.py file with your local path to the sales_data.csv file
FILE_PATH = "your_file_path_here" 


Step 3: 
- Get an API key by signing up for a free account from api.openweathermap.org. Once you sign up and get your API key then update that in the Config.py file
- You can use the endpoint: `https://api.openweathermap.org/data/2.5/weather`
# API key for the weather API (OpenWeatherMap)
API_KEY = "your_api_key_here"

Step 4:
Install SQLite extension if you are using Visual Studio Code to run your code

Step 5: Run the Data Pipeline

Run the data_pipeline.py script from bash window:
 
python data_pipeline.py

This will execute the data pipeline, perform data transformations, aggregations, and store the transformed data in the SQLite database (sales_data.db).

You can also use Run feature in Visual Studio Code.

Data Transformation Steps:

Read sales data from the CSV file specified in the FILE_PATH.
Fetch user data from the JSONPlaceholder API (https://jsonplaceholder.typicode.com/users) and merge it with the sales data based on customer_id.
Fetch weather data for each location (latitude and longitude) from the OpenWeatherMap API and append the temperature and weather conditions to the merged data.
Assumptions:

The OpenWeatherMap API provides weather data based on latitude and longitude.
Database Schema:
The transformed data is stored in five tables in the SQLite database sales_data.db:

CustomerSales: Stores total sales amount per customer.
SalesTrends: Stores total quantity of sales per month.
ProductQuantity: Stores the average order quantity per product.
TopSellingProducts: Stores the total quantity sold for the top 10 selling products.
SalesDataTransformed: Stores the fully transformed data, including weather information.
Aggregations and Data Manipulations:

The data pipeline performs the following aggregations and manipulations:

Calculates total sales amount per customer based on the quantity and price of each product sold.
Determines the average order quantity per product.
Identifies the top-selling products based on the total quantity sold.
Finds the top 5 customers based on total sales amount.
Analyzes sales trends over time by calculating the total quantity of sales per month.
 