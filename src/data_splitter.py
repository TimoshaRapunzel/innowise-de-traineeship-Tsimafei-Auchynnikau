import pandas as pd
import os

def split_by_month(input_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    reader = pd.read_csv(input_path, chunksize=500000, skipinitialspace=True)
    
    for i, chunk in enumerate(reader):
        chunk['departure'] = pd.to_datetime(chunk['departure'])
        chunk['month_key'] = chunk['departure'].dt.strftime('%Y-%m')
        
        for month, data in chunk.groupby('month_key'):
            file_path = os.path.join(output_dir, f"{month}.csv")
            data.drop(columns=['month_key']).to_csv(
                file_path, mode='a', index=False, header=not os.path.exists(file_path)
            )
            print(f"Processed chunk {i} for {month}")

if __name__ == "__main__":
    split_by_month('/opt/airflow/data/database.csv', '/opt/airflow/data/split/')