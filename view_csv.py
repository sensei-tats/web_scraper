import pandas as pd

# Replace this with your actual filename (the timestamp will be different)
filename = "scraped_data_20250414_235042.csv"

try:
    df = pd.read_csv(filename)
    print(df)
except FileNotFoundError:
    print(f"Error: The file {filename} was not found.")
except pd.errors.EmptyDataError:
    print(f"Error: The file {filename} is empty.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
