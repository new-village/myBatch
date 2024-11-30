import os
import sys
import pyarrow as pa
import pyarrow.parquet as pq
import keibascraper as ks  # keibascraperライブラリからデータを取得する関数

def main():
    # コマンドライン引数からレースIDを取得
    if len(sys.argv) < 2:
        print("Usage: python -m batch_scripts.keibascraper <race_id>")
        sys.exit(1)

    race_id = sys.argv[1]

    try:
        print(f"Fetching data for race ID: {race_id}")

        # keibascraperを使ってデータを取得
        race_info = ks.load('entry', race_id)

        # 出力ディレクトリの確認または作成
        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)

        # parquet形式で保存
        output_path = os.path.join(output_dir, f"{race_id}.parquet")

        # ネストされたデータをParquet形式で保存
        table = pa.Table.from_pylist([race_info])
        pq.write_table(table, output_path)

        print(f"Data for race ID {race_id} saved to {output_path}")

    except Exception as e:
        print(f"Error processing race ID {race_id}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()