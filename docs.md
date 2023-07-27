# Sales Data Pipeline

This data pipeline is designed to combine sales data with data from external sources, perform data transformations and aggregations, and store the final dataset in a database. The pipeline enables analysis and derivation of insights into customer behavior and sales performance.

## Setup Instructions

1. Install the required Python packages:
```console
pip install -r requirements.txt
``` DONE

2. Obtain API keys:
- OpenWeatherMap API key: [OpenWeatherMap API](https://openweathermap.org/)
DONE

3. Clone the repository:

```
git clone https://github.com/your-username/sales-data-pipeline.git
```

4. Update the configuration:
- Open `config.py` and replace `<YOUR_API_KEY>` with your OpenWeatherMap API key.

5. Generate the sales data:
```
python generate_sales_data.py
```

6. Perform data transformations and aggregations:
```console
python data_pipeline.py
```

7. Check the database:
- The transformed and aggregated data will be stored in the database file `sales_data.db`.

## Data Pipeline Components

The data pipeline consists of the following components:

- `generate_sales_data.py`: Generates the sales data in CSV format.

- `data_pipeline.py`: Implements the data pipeline by performing data transformations, aggregations, and storing the data in a database.

- `config.py`: Contains the configuration settings for the data pipeline, including API keys and database settings.

- `sales_data.csv`: The generated sales data file.

## Aggregations and Data Manipulation Tasks

The following aggregations and data manipulation tasks are performed in the data pipeline:

1. Calculate total sales amount per customer.
2. Determine the average order quantity per product.
3. Identify the top-selling products.
4. Identify the top customers.

## Database Schema

The transformed and aggregated data is stored in a SQLite database with the following schema:

**CustomerSales Table**
- customer_id INTEGER PRIMARY KEY
- total_sales_amount REAL

**ProductQuantities Table**
- product_id INTEGER PRIMARY KEY
- total_quantity INTEGER
- total_orders INTEGER

## Conclusion

The sales data pipeline successfully combines data from multiple sources, performs transformations and aggregations, and stores the final dataset in a database. By running the pipeline and analyzing the data, valuable insights can be derived to understand customer behavior and sales performance.

For any questions or issues, please contact [tiago.pessoa@g42.ai.com](mailto:tiago.pessoa@g42.ai.com).
