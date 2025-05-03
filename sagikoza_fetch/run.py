import sys
import sagikoza
import pandas as pd
import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq

def setup_logger():
    """ロガーの設定を行う"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def get_output_file():
    """出力ファイルのパスを返す"""
    output_dir = "/data"
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        return os.path.join(output_dir, "sagikoza.parquet")
    except Exception as e:
        logging.error(f"出力ディレクトリ作成・取得時にエラー: {e}")
        raise

def fetch_data(year=None):
    """データ取得処理"""
    try:
        if year is None:
            data = sagikoza.fetch()
        else:
            data = sagikoza.fetch(year)
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"データ取得時にエラー: {e}")
        raise

def save_data(df, output_file):
    """データをParquet形式で保存（append対応、低メモリ）"""
    try:
        table = pa.Table.from_pandas(df)
        if os.path.exists(output_file):
            # 既存ファイルにappend
            with pq.ParquetWriter(output_file, table.schema, use_dictionary=True, compression='snappy') as writer:
                # 既存データは読み込まず、新規データのみappend
                writer.write_table(table)
        else:
            pq.write_table(table, output_file, use_dictionary=True, compression='snappy')
        logging.info(f"Saved to {output_file}")
    except Exception as e:
        logging.error(f"データ保存時にエラー: {e}")
        raise

def main():
    setup_logger()
    try:
        year = sys.argv[1] if len(sys.argv) > 1 else None
        df = fetch_data(year)
        logging.info(f"取得データ件数: {len(df)} 件")
        output_file = get_output_file()
        save_data(df, output_file)
    except Exception as e:
        logging.error(f"メイン処理でエラー: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
