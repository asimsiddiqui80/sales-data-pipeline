import csv
import random
from datetime import datetime, timedelta

def generate_sales_data(num_rows):
    data = []
    
    cities = ['London', 'Paris', 'New York', 'Tokyo', 'Sydney']
    countries = ['UK', 'France', 'USA', 'Japan', 'Australia']
    
    for _ in range(num_rows):
        order_id = random.randint(1000, 9999)
        customer_id = random.randint(1, 10)
        product_id = random.randint(1, 50)
        quantity = random.randint(1, 10)
        price = round(random.uniform(10.0, 100.0), 2)
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        
        # Select a random city and country
        index = random.randint(0, len(cities) - 1)
        
        row = [
            order_id,
            customer_id,
            product_id,
            quantity,
            price,
            order_date.strftime("%Y-%m-%d"),
        ]
        
        data.append(row)
    
    return data

def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['order_id', 'customer_id', 'product_id', 'quantity', 'price', 'order_date'])
        writer.writerows(data)

# Generate 1000 rows of sales data
sales_data = generate_sales_data(1000)

# Save the data to a CSV file
save_to_csv(sales_data, 'sales_data.csv')

print("Sales data CSV generated successfully.")
