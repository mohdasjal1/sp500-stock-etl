from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import requests
import os

# -----------------------------------------
# Constants
# -----------------------------------------
SP500_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
S3_BUCKET = 'sp500-stock-data'   
S3_FOLDER = 'stock'

# Data processing settings
MAX_SYMBOLS_FOR_TESTING = None  # Set to None for all symbols
DATA_RETENTION_DAYS = 1       # How many days of historical data to fetch

# -----------------------------------------
# Functions
# -----------------------------------------
def extract_symbols(ti, **kwargs):
    """Extract S&P 500 symbols from Wikipedia with enhanced validation"""
    try:
        print("Extracting S&P 500 symbols from Wikipedia...")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        response = requests.get(SP500_URL, headers=headers, timeout=30)
        response.raise_for_status()

        tables = pd.read_html(response.text)

        if not tables:
            raise ValueError("No tables found in the Wikipedia page")

        # Get the first table (S&P 500 companies)
        df = tables[0]

        # Validate required columns
        if 'Symbol' not in df.columns:
            raise ValueError("Symbol column not found in the table")

        # Clean and validate symbols
        symbols = df['Symbol'].dropna().tolist()
        symbols = [symbol.strip() for symbol in symbols if symbol.strip()]

        # Remove any invalid symbols 
        valid_symbols = []
        for symbol in symbols:
            if len(symbol) <= 5 and symbol.replace('.', '').replace('-', '').isalnum():
                valid_symbols.append(symbol)
            else:
                print(f"Skipping invalid symbol: {symbol}")

        print(f"Extracted {len(valid_symbols)} valid symbols from {len(symbols)} total")

        if not valid_symbols:
            raise ValueError("No valid symbols found")

        # For testing, limit symbols. Set MAX_SYMBOLS_FOR_TESTING to None for production
        if MAX_SYMBOLS_FOR_TESTING:
            symbols_to_process = valid_symbols[:MAX_SYMBOLS_FOR_TESTING]
        else:
            symbols_to_process = valid_symbols
        print(f"Processing {len(symbols_to_process)} symbols for this run")

        # Send to XCom
        ti.xcom_push(key='symbols', value=symbols_to_process)
        ti.xcom_push(key='total_symbols_found', value=len(valid_symbols))

        return symbols_to_process

    except requests.RequestException as e:
        print(f"Network error while fetching symbols: {str(e)}")
        raise
    except Exception as e:
        print(f"Error extracting symbols: {str(e)}")
        raise


def get_stock_data(**context):
    """Fetch stock data for symbols with enhanced data quality"""
    symbols = context['ti'].xcom_pull(key='symbols', task_ids='extract_symbols')
    all_data = []

    print(f"Processing {len(symbols)} symbols...")

    for symbol in symbols:
        try:
            print(f"Fetching data for {symbol}...")

            # Download data with better error handling
            df = yf.download(
                tickers=symbol,
                start=datetime.now() - timedelta(days=DATA_RETENTION_DAYS),
                end=datetime.now(),
                interval="1d",
                progress=False,
                auto_adjust=False,  # Explicitly set to avoid warning
                prepost=False,       # Don't include pre/post market data
                threads=True         # Use threading for better performance
            )

            if df.empty:
                print(f"No data available for {symbol}")
                continue

            # Debug: Print DataFrame info
            print(f"{symbol} - DataFrame shape: {df.shape}")
            print(f"{symbol} - Columns: {list(df.columns)}")

            # Handle multi-level columns properly
            if isinstance(df.columns, pd.MultiIndex):
                print(f"{symbol} - MultiIndex columns detected, flattening...")
                # Flatten multi-level columns - keep only the first level (column name)
                df.columns = [col[0] for col in df.columns]
                print(f"{symbol} - Flattened columns: {list(df.columns)}")

            # Reset index to convert Date from index to column
            df = df.reset_index()
            print(f"{symbol} - After reset_index: {list(df.columns)}")

            # Ensure Date column exists and format it properly
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
            else:
                print(f"No Date column found for {symbol}, available columns: {list(df.columns)}")
                continue

            # Add symbol column
            df["Symbol"] = symbol

            # Standardize column names (handle different naming conventions)
            column_mapping = {
                'Open': 'Open',
                'High': 'High',
                'Low': 'Low',
                'Close': 'Close',
                'Adj Close': 'Adj_Close',
                'AdjClose': 'Adj_Close',  # Alternative naming
                'Volume': 'Volume'
            }

            # Rename columns to standard names
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename(columns={old_name: new_name})
                    print(f"{symbol} - Renamed {old_name} to {new_name}")

            print(f"{symbol} - Final columns before processing: {list(df.columns)}")

            # Calculate additional metrics
            if 'Close' in df.columns:
                df['Close_Change'] = df['Close'].diff().fillna(0)
                df['Close_Pct_Change'] = (df['Close'].pct_change().fillna(0) * 100)

                # Calculate daily range
                if 'High' in df.columns and 'Low' in df.columns:
                    df['Daily_Range'] = df['High'] - df['Low']
                    df['Daily_Range_Pct'] = ((df['High'] - df['Low']) / df['Low'] * 100).fillna(0)

            # Ensure all numeric columns are properly formatted
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume',
                           'Close_Change', 'Close_Pct_Change', 'Daily_Range', 'Daily_Range_Pct']

            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Round to appropriate decimal places
                    if col in ['Close_Change', 'Close_Pct_Change', 'Daily_Range', 'Daily_Range_Pct']:
                        df[col] = df[col].round(4)
                    else:
                        df[col] = df[col].round(2)

            # Handle missing values properly (don't fill with 0, use NULL)
            df = df.replace([pd.NA, pd.NaT], None)

            # Remove rows where essential data is missing
            df = df.dropna(subset=['Date', 'Symbol', 'Close'])

            # Define final column order for clean CSV (matching Snowflake table schema)
            final_columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close',
                           'Volume', 'Close_Change', 'Close_Pct_Change', 'Daily_Range', 'Daily_Range_Pct']

            # Only select columns that exist and have data
            available_columns = [col for col in final_columns if col in df.columns]
            df = df[available_columns]

            if not df.empty:
                all_data.append(df)
                print(f"{symbol}: {len(df)} rows, columns: {list(df.columns)}")
            else:
                print(f"{symbol}: No valid data after processing")

        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            import traceback
            print(f"Full traceback: {traceback.format_exc()}")
            continue

    if not all_data:
        raise ValueError("No data fetched for any symbols!")

    # Combine all data
    final_df = pd.concat(all_data, ignore_index=True)

    # Sort by date and symbol for organized data
    final_df = final_df.sort_values(['Date', 'Symbol']).reset_index(drop=True)

    # Data quality checks
    print(f"\nData Quality Summary:")
    print(f"Total rows: {len(final_df)}")
    print(f"Unique symbols: {final_df['Symbol'].nunique()}")
    print(f"Date range: {final_df['Date'].min()} to {final_df['Date'].max()}")
    print(f"Missing values per column:")
    for col in final_df.columns:
        missing_count = final_df[col].isnull().sum()
        if missing_count > 0:
            print(f"  {col}: {missing_count}")

    # Generate CSV filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = f"/tmp/sp500_data_{timestamp}.csv"

    # Save with enhanced CSV formatting
    final_df.to_csv(
        csv_path,
        index=False,
        quoting=1,  # QUOTE_ALL for Snowflake compatibility
        encoding='utf-8',
        float_format='%.4f',  # More precision for financial data
        na_rep='',  # Empty string for NULL values
        date_format='%Y-%m-%d'
    )

    context['ti'].xcom_push(key='csv_path', value=csv_path)
    context['ti'].xcom_push(key='row_count', value=len(final_df))
    context['ti'].xcom_push(key='symbol_count', value=final_df['Symbol'].nunique())

    print(f"\nData saved to {csv_path}")
    print(f"File size: {os.path.getsize(csv_path) / 1024:.2f} KB")

    # Show data preview
    print(f"\nData Preview (first 3 rows):")
    print(final_df.head(3).to_string(index=False))

    return csv_path


def save_and_upload(**context):
    """Upload CSV to S3 with enhanced error handling"""
    csv_path = context['ti'].xcom_pull(key='csv_path', task_ids='get_stock_data')
    row_count = context['ti'].xcom_pull(key='row_count', task_ids='get_stock_data')
    symbol_count = context['ti'].xcom_pull(key='symbol_count', task_ids='get_stock_data')

    if not csv_path:
        raise ValueError("No CSV path received from previous task")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    file_name = os.path.basename(csv_path)
    file_size = os.path.getsize(csv_path)

    print(f"Uploading file: {file_name}")
    print(f"File size: {file_size / 1024:.2f} KB")
    print(f"Rows: {row_count}, Symbols: {symbol_count}")

    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')  # Change to your AWS connection ID

        # Upload to S3
        s3_hook.load_file(
            filename=csv_path,
            key=f"{S3_FOLDER}/{file_name}",
            bucket_name=S3_BUCKET,
            replace=True
        )

        print(f"Successfully uploaded to s3://{S3_BUCKET}/{S3_FOLDER}/{file_name}")

        # Store S3 path for Snowflake task
        s3_path = f"s3://{S3_BUCKET}/{S3_FOLDER}/{file_name}"
        context['ti'].xcom_push(key='s3_path', value=s3_path)
        context['ti'].xcom_push(key='file_name', value=file_name)

    except Exception as e:
        print(f"S3 upload failed: {str(e)}")
        raise

    # Clean up temp file
    try:
        os.remove(csv_path)
        print(f"Cleaned up temp file: {csv_path}")
    except Exception as e:
        print(f"Could not delete temp file: {e}")


# -----------------------------------------
# Default args & DAG definition
# -----------------------------------------
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
}

with DAG(
    dag_id='sp500_pipeline_dag',
    default_args=default_args,
    description='S&P 500 Stock Data Pipeline - Extract, Transform, Load to S3 and Snowflake',
    schedule='0 0 * * *',  # Runs every day at midnight
    catchup=False,
    max_active_runs=1,
    tags=['stocks', 'sp500', 'aws', 'snowflake', 'etl', 'financial_data']
) as dag:

    start = EmptyOperator(task_id='start')

    extract_symbols_task = PythonOperator(
        task_id='extract_symbols',
        python_callable=extract_symbols,
    )

    get_stock_data_task = PythonOperator(
        task_id='get_stock_data',
        python_callable=get_stock_data,
    )

    save_and_upload_task = PythonOperator(
        task_id='save_and_upload',
        python_callable=save_and_upload,
    )

    # Load data task - simplified since table and stage already exist
    load_to_snowflake_task = SQLExecuteQueryOperator(
        task_id='load_to_snowflake',
        conn_id='snowflake_default',
        sql="""
        -- Copy data from S3 to Snowflake table using existing stage
        COPY INTO STOCK_DATA (
            DATE,
            SYMBOL,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            CLOSE_CHANGE,
            CLOSE_PCT_CHANGE,
            DAILY_RANGE,
            DAILY_RANGE_PCT
        )
        FROM @my_s3_stage
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null', '', '\\N')
            FIELD_DELIMITER = ','
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            EMPTY_FIELD_AS_NULL = TRUE
        )
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE;

        -- Verify the load
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT SYMBOL) as unique_symbols,
            MIN(DATE) as earliest_date,
            MAX(DATE) as latest_date,
        FROM STOCK_DATA;
        """
    )

    end = EmptyOperator(task_id='end')

    # DAG dependencies
    start >> extract_symbols_task >> get_stock_data_task >> save_and_upload_task >> load_to_snowflake_task >> end