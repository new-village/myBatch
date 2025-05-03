import sys
import sagikoza_collector
import pandas as pd
import os

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # 引数が無い場合は fetch() を引数なしで実行
        data = sagikoza_collector.fetch()
    else:
        year = sys.argv[1]
        data = sagikoza_collector.fetch(year)
    df = pd.DataFrame(data)
    # dataディレクトリの作成
    output_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "sagikoza.parquet")

    if os.path.exists(output_file):
        # 既存ファイルがあればappend
        existing_df = pd.read_parquet(output_file)
        df = pd.concat([existing_df, df], ignore_index=True)
    # 保存（新規または上書き）
    df.to_parquet(output_file, index=False)
    print(f"Saved to {output_file}")
