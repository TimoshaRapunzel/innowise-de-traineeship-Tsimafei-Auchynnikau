import pandas as pd
import random
from datetime import datetime, timedelta
import os

TOTAL_ROWS = 1000
OUTPUT_DIR = 'data'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_mock_data(n_rows):
    print(f"Generating {n_rows} rows of mock data...")
    
    cities_regions = [
        ('New York City', 'East'), ('Los Angeles', 'West'), ('Chicago', 'Central'),
        ('Houston', 'Central'), ('Phoenix', 'West'), ('Philadelphia', 'East'),
        ('San Antonio', 'Central'), ('San Diego', 'West'), ('Dallas', 'Central'),
        ('San Jose', 'West')
    ]
    
    customers = [f"Customer_{i}" for i in range(1, 101)]
    products = [f"Product_{i}" for i in range(1, 51)]
    
    data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(n_rows):
        order_id = f"ORD-{10000 + i}"
        customer_id = f"CUST-{random.randint(1, 100):03d}"
        customer_name = f"Customer Name {customer_id.split('-')[1]}"
        city, region = random.choice(cities_regions)
        product_id = random.choice(products)
        sales = round(random.uniform(10.0, 1000.0), 2)
        order_date = start_date + timedelta(days=random.randint(0, 365))
        
        data.append([order_id, customer_id, customer_name, region, city, product_id, sales, order_date])
        
    columns = ['Order ID', 'Customer ID', 'Customer Name', 'Region', 'City', 'Product ID', 'Sales', 'Order Date']
    df = pd.DataFrame(data, columns=columns)
    return df

def main():
    if os.path.exists('Superstore.csv'):
        print("Loading existing Superstore.csv...")
        df = pd.read_csv('Superstore.csv')
        if 'City' not in df.columns:
             df['City'] = 'Unknown'
    else:
        df = generate_mock_data(TOTAL_ROWS)
        df.to_csv('Superstore_Mock.csv', index=False)
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df = df.sort_values('Order Date')
    split_idx = int(len(df) * 0.8)
    
    initial_load = df.iloc[:split_idx].copy()
    secondary_data = df.iloc[split_idx:].copy()
    
    print(f"Initial load size: {len(initial_load)}")
    print(f"Base Secondary load size: {len(secondary_data)}")
    

    n_dupes = random.randint(3, 5)
    duplicates = initial_load.sample(n_dupes)
    print(f"Injecting {n_dupes} duplicates.")
    secondary_data = pd.concat([secondary_data, duplicates])

    scd1_candidates = initial_load['Customer ID'].unique()
    if len(scd1_candidates) > 0:
        cust_id_scd1 = random.choice(scd1_candidates)
        new_row_scd1 = secondary_data.iloc[0].copy()
        new_row_scd1['Order ID'] = f"ORD-SCD1-{random.randint(1000,9999)}"
        new_row_scd1['Customer ID'] = cust_id_scd1
        new_row_scd1['Customer Name'] = f"Customer Name {cust_id_scd1.split('-')[1]} (Corrected)"
        new_row_scd1['Order Date'] = secondary_data['Order Date'].max()
        print(f"Injecting SCD1 for {cust_id_scd1}: Name changed to {new_row_scd1['Customer Name']}")
        secondary_data = pd.concat([secondary_data, pd.DataFrame([new_row_scd1])])

    scd2_candidates = initial_load['Customer ID'].unique()
    if len(scd2_candidates) > 0:
        cust_id_scd2 = random.choice([c for c in scd2_candidates if c != cust_id_scd1])
        
        new_row_scd2 = secondary_data.iloc[0].copy()
        new_row_scd2['Order ID'] = f"ORD-SCD2-{random.randint(1000,9999)}"
        new_row_scd2['Customer ID'] = cust_id_scd2
        original_name = initial_load[initial_load['Customer ID'] == cust_id_scd2].iloc[0]['Customer Name']
        new_row_scd2['Customer Name'] = original_name
        new_row_scd2['City'] = 'Miami'
        new_row_scd2['Region'] = 'South'
        new_row_scd2['Order Date'] = secondary_data['Order Date'].max()
        
        print(f"Injecting SCD2 for {cust_id_scd2}: Moved to Miami, South")
        secondary_data = pd.concat([secondary_data, pd.DataFrame([new_row_scd2])])

    initial_load.to_csv(os.path.join(OUTPUT_DIR, 'initial_load.csv'), index=False)
    secondary_data.to_csv(os.path.join(OUTPUT_DIR, 'secondary_load.csv'), index=False)
    print("Files saved to ./data/")

if __name__ == "__main__":
    main()
