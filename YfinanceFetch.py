import yfinance as yf
import pandas as pd
import numpy as np
import boto3
from io import BytesIO
import logging
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# List of stocks to fetch
stocks = ["AAPL", "GOOG", "MSFT", "AMZN", "NVDA", "BTC", "MNDY", "INTC", "UNH", "META", "JNJ", "MA"]


# Fetch data using yfinance
def fetch_stock_data(stocks):
    data_frames = []
    for stock in stocks:
        try:
            logger.info(f"Fetching data for {stock}")
            t = yf.Ticker(stock)
            stock_data = t.history(period="1y")
            stock_data['stock'] = stock  # Add a column for stock symbol
            data_frames.append(stock_data)
        except Exception as e:
            logger.error(f"Error fetching data for {stock}: {e}")
            continue  # Continue fetching data for the next stock even if one fails

    if len(data_frames) == 0:
        logger.error("No data was fetched. Exiting process.")
        raise ValueError("No stock data fetched.")

    # Concatenate all stock data into a single DataFrame
    all_data = pd.concat(data_frames, axis=0)
    return all_data


# Preprocess the data
def preprocess_data(data):
    try:
        # Reset index and ensure 'Date' column is available
        data.reset_index(inplace=True)

        # Round all decimal values to 2 decimal places
        data = data.round({'Open': 2, 'High': 2, 'Low': 2, 'Close': 2})

        # Retain only relevant columns
        data = data[['Date', 'Open', 'High', 'Low', 'Close']]

        # Drop rows with NaN values
        data = data.dropna()

        # Calculate 'gap' column: difference between high and low
        data['gap'] = data['High'] - data['Low']

        # Create 'gap_categories' column based on 'gap'
        data['gap_categories'] = pd.cut(data['gap'], bins=[-np.inf, 4, 8, np.inf], labels=['low', 'medium', 'high'])

        # Calculate moving average and standard deviation (3-day window)
        data['gap_ma'] = data['gap'].rolling(window=3, min_periods=1).mean()
        data['gap_std'] = data['gap'].rolling(window=3, min_periods=1).std()

        # For rows where the standard deviation is NaN (the first row), fill with the gap value
        data['gap_std'] = data['gap_std'].fillna(data['gap'])

        # Format the date column to YYYY-mm-dd format
        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')

        return data
    except KeyError as e:
        print(e)
        logger.error(f"Missing expected column in the data: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during data preprocessing: {e}")
        raise


# Save data to S3 in Parquet format, partitioned by day
def save_to_s3(data, bucket_name, aws_access_key_id, aws_secret_access_key, endpoint_url):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      endpoint_url=endpoint_url)

    try:
        # Group by 'Date' and save each partitioned file
        for date, group in data.groupby('Date'):
            file_buffer = BytesIO()
            # Save each group as a parquet file
            group.drop(columns='Date').to_parquet(file_buffer, index=False, engine='pyarrow')
            file_buffer.seek(0)

            # Save the parquet file to S3- I choose Parquet because it is a columnar format, making it more efficient for both storage and querying large datasets.
            # Supports partitioning, which is useful when dealing with large volumes of time-series data like stock prices.
            s3.put_object(Bucket=bucket_name, Key=f"{date}/stock_data.parquet", Body=file_buffer)

        logger.info(f"Data successfully uploaded to {bucket_name}")

    except boto3.exceptions.S3UploadFailedError as e:
        logger.error(f"S3 upload failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while uploading data to S3: {e}")
        raise


# Main function to run the full pipeline
def main():
    try:
        load_dotenv()
        # S3 credentials and endpoint
        aws_access_key_id = os.environ["aws_access_key_id"]
        aws_secret_access_key = os.environ["aws_secret_access_key"]
        endpoint_url = "https://finance-stock-interview.s3.ca-central-1.amazonaws.com"
    except FileNotFoundError:
        # Terminate the script if no config files are found
        print("CONFIG FILE(s) NOT FOUND. TERMINATING.")
    try:
        # Fetch the stock data
        all_data = fetch_stock_data(stocks)

        # Preprocess the data
        cleaned_data = preprocess_data(all_data)

        # Save data to S3
        save_to_s3(cleaned_data, 'finance-stock', aws_access_key_id, aws_secret_access_key, endpoint_url)

    except Exception as e:
        logger.error(f"Process failed: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Process failed with error: {e}")
