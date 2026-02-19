import pandas as pd
import os
from datetime import datetime

class CsvSplitter:

    def __init__(self, input_path: str, output_dir: str):
        self.input_path = input_path
        self.output_dir = output_dir

    def process(self):

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        chunk_size = 1000000
        for chunk in pd.read_csv(self.input_path, chunksize=chunk_size):

            chunk['Departure'] = pd.to_datetime(chunk['Departure'])
            chunk['year_month'] = chunk['Departure'].dt.to_period('M')

            for period, data in chunk.groupby('year_month'):
                filename = f"{period}.csv"
                file_path = os.path.join(self.output_dir, filename)
                header = not os.path.exists(file_path)
                data.drop(columns=['year_month']).to_csv(file_path, mode='a', header=header, index=False)
                print(f"Processed chunk for {filename}")