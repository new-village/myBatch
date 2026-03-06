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

def fetch_full():
    """
    Downloads all corporate registrations for the current month.
    Saves to /data/corpreg_nta_YYYYMM.parquet
    """
    client = CorporateRegistryClient()
    exec_date = pd.Timestamp.now()
    output_filename = f"corpreg_nta_{exec_date.strftime('%Y%m')}.parquet"
    output_path = os.path.join("/data", output_filename)

    logger.info("Starting full data fetch...")
    
    # fetch returns the path to the parquet directory if format="parquet"
    # jpcorpreg's fetch() without arguments downloads all prefectures.
    temp_dir = client.fetch(format="parquet")
    
    logger.info(f"Data downloaded to temporary directory: {temp_dir}")

    # Use DuckDB to merge partitioned parquet files into a single file
    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM read_parquet('{temp_dir}/**/*.parquet')) TO '{output_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')")
    
    # Verification
    cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
    logger.info(f"Successfully saved {cnt} records to {output_path}")

if __name__ == "__main__":
    fetch_full()
