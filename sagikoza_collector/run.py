import sys
import sagikoza
import pandas as pd
import os
import logging

def setup_logger():
    """ロガーの設定を行う"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def get_output_file():
    """出力ファイルのパスを返す"""
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    output_dir = os.path.join(root_dir, "data")
    os.makedirs(output_dir, exist_ok=True)
    return os.path.join(output_dir, "sagikoza.parquet")

def fetch_data(year=None):
    """データ取得処理"""
    if year is None:
        data = sagikoza.fetch()
    else:
        data = sagikoza.fetch(year)
    return pd.DataFrame(data)

def save_data(df, output_file):
    """データをParquet形式で保存（既存があればappend）"""
    if os.path.exists(output_file):
        existing_df = pd.read_parquet(output_file)
        df = pd.concat([existing_df, df], ignore_index=True)
    df.to_parquet(output_file, index=False)
    logging.info(f"Saved to {output_file}")

def main():
    setup_logger()
    year = sys.argv[1] if len(sys.argv) > 1 else None
    df = fetch_data(year)
    logging.info(f"取得データ件数: {len(df)} 件")
    output_file = get_output_file()
    save_data(df, output_file)

if __name__ == "__main__":
    main()
