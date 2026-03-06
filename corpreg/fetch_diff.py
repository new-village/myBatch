import os
import sys
import logging
import pandas as pd
from jpcorpreg import CorporateRegistryClient
import duckdb

# Set up logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def fetch_diff(target_date: str | None = None):
    """
    Downloads differential updates for a specific date.
    Args:
        target_date: Date in YYYYMMDD format. Defaults to yesterday.
    Saves to /data/corpreg_nta_YYYYMMDD.parquet
    """
    client = CorporateRegistryClient()
    
    if target_date is None:
        # Default to yesterday
        target_date = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y%m%d")
    
    output_filename = f"corpreg_nta_{target_date}.parquet"
    output_path = os.path.join("/data", output_filename)

    logger.info(f"Starting differential data fetch for date: {target_date}")
    
    # fetch_diff returns the path to the parquet directory if format="parquet"
    temp_dir = client.fetch_diff(target_date, format="parquet")
    
    logger.info(f"Differential data downloaded to temporary directory: {temp_dir}")

    # Use DuckDB to merge partitioned parquet files into a single file
    con = duckdb.connect()
    # Note: fetch_diff might return an empty directory if no updates exist. 
    # read_parquet might fail if no files match.
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet('{temp_dir}/**/*.parquet')) TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')")
        cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
        logger.info(f"Successfully saved {cnt} records to {output_path}")
    except Exception as e:
        logger.warning(f"Failed to process differential data: {e}. It might be that no data was found for the date.")

if __name__ == "__main__":
    # Allow passing date as argument
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    fetch_diff(date_arg)
