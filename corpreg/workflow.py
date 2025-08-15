import os
import sys
import logging
import pandas as pd
import jpcorpreg
from ja_entityparser import corporate_parser
from concurrent.futures import ProcessPoolExecutor
import pyarrow as pa

# Set up logger object
logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
logger = logging.getLogger(__name__)


def load_parquet(filename: str, memory_map: bool = True) -> pd.DataFrame:
    """/data 配下に単一の .parquet ファイルを読み込む関数。

    - filename: /data 直下またはサブディレクトリのファイル名。
    - columns: 読み込む列を限定（省メモリ）
    - memory_map: OS のメモリマップを使用（省コピー）

    例:
        df = load_parquet("corporate_registry_202508.parquet", columns=["corporate_number","name"])
        df = load_parquet("subdir/file.parquet")
    """
    full_path = os.path.join("/data", filename)
    if os.path.exists(full_path):
        df = pd.read_parquet(
            full_path,
            engine="pyarrow",
            dtype_backend="pyarrow",
            memory_map=memory_map,
        )
        logger.info(f"Loaded from {full_path}: {df.shape}")
    else:
        df = pd.DataFrame(columns=["corporate_number", "name"])
        logger.warning(f"File not found: {full_path}. Returning empty DataFrame.")
    return df

def save_parquet(df: pd.DataFrame, filename: str) -> None:
    """/data 配下に単一の .parquet ファイルを保存する関数。"""
    full_path = os.path.join("/data", filename)
    df.to_parquet(
        full_path,
        engine="pyarrow",
        compression="zstd",        # snappy でも可。zstd は圧縮率が高め
        use_dictionary=True,       # 文字列などを辞書エンコード
    )
    logger.info(f"Saved to {full_path}: {df.shape}")

def fetch(prefecture:str = "ALL") -> pd.DataFrame:
    """法人番号公表サイトから法人情報を取得する関数

    例:
        df = fetch_data()
    """
    # 日付を付与してファイル名を決定
    exec_date = pd.Timestamp.now().strftime("%Y%m")
    logger.info(f"Loading corporate registry as of {exec_date}")

    # 法人情報を取得して保存
    return jpcorpreg.load(prefecture=prefecture)

def merge(new: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    """新しい法人情報と古い法人情報をマージする関数

    例:
        merged = merge(new, old)
    """
    if base.empty:
        logger.warning("Skip merging with empty base DataFrame.")
        return new

    # 新旧のデータフレームを単純に縦結合して、重複を削除
    merged = pd.concat([new, base], axis=0, ignore_index=True)
    merged.drop_duplicates(subset=["corporate_number", "update_date"],
                       keep="first", inplace=True, ignore_index=True)

    # Arrow 拡張配列だと groupby idxmax が未実装のため、update_date を pandas ネイティブ型へ
    merged["update_date"] = merged["update_date"].astype(pd.StringDtype(storage="python"))

    # 全行の latest=0 で初期化
    merged["latest"] = 0
    # corporate_number ごとに update_date が最大の行を latest=1 に設定
    idx = merged.groupby("corporate_number", sort=False)["update_date"].idxmax()
    merged.loc[idx, "latest"] = 1
    
    logger.info(f"Merged DataFrame shape: {merged.shape}")
    return merged

def _parse_name_worker(name: object) -> dict:
    # 子プロセス側で安全に Sudachi を使うためのワーカー
    if not isinstance(name, str) or not name.strip():
        return {"legal_form": None, "brand_name": None}
    try:
        res = corporate_parser(name)
        return {
            "legal_form": res.get("legal_form"),
            "brand_name": res.get("brand_name"),
            "brand_kana": res.get("brand_kana", ""),
        }
    except Exception:
        # 例外は親に伝播させず欠損扱い
        return {"legal_form": None, "brand_name": None, "brand_kana": None}

def enrich_name(df: pd.DataFrame) -> pd.DataFrame:
    """法人名を補完する関数

    例:
        df = enrich_name(df)
    """
    # プロセス並列で Sudachi のグローバル共有を回避
    names = df["name"].tolist() if "name" in df.columns else []
    with ProcessPoolExecutor(max_workers=4) as ex:
        rows = list(ex.map(_parse_name_worker, names))
    parsed = pd.DataFrame(rows, index=df.index)
    df["legal_form"] = parsed["legal_form"]
    df["brand_name"] = parsed["brand_name"]
    df["brand_kana"] = parsed["brand_kana"]
    logger.info(f"Enrich DataFrame shape: {df.shape}")
    return df

def legal_form_stat(df: pd.DataFrame) -> None:
    """法人名の報告を行う関数
    例:
        legal_form_stat(df)
    """
    lf_stat = df[
        (df['latest'] == 1) &
        (df['kind'] != '101') &
        (df['kind'] != '201') &
        (df['kind'] != '399') &
        (df['kind'] != '499') &
        (df['legal_form'].isnull() | (df['legal_form'] == ''))
    ]

    logger.info(f"Unexpected legal form records: {lf_stat.shape[0]}")
    cols = ['corporate_number', 'name', 'legal_form', 'brand_name']
    return lf_stat[cols]

def missing_kanji_stat(df: pd.DataFrame) -> None:
    """法人名の報告を行う関数
    例:
        missing_kanji_stat(df)
    """
    mk_stat = df[df["name"].astype(str).str.contains("_", na=False, regex=False)]
    logger.info(f"Missing kanji (underscore in name) records: {mk_stat.shape[0]}")
    cols = ['corporate_number', 'name', 'name_image_id']
    return mk_stat[cols]

if __name__ == "__main__":
    exec_date = pd.Timestamp.now().strftime("%Y%m%d")

    # 引数処理
    if len(sys.argv) > 1:
        prefecture = sys.argv[1].upper()
    else:
        prefecture = "ALL"

    # 法人番号サイトからデータを取得して保存
    new = fetch(prefecture)
    save_parquet(new, f"corpreg_nta_{exec_date[:6]}.parquet")
    # 前回実行分のデータを読み込み
    new = load_parquet(f"corpreg_nta_{exec_date[:6]}.parquet")
    base = load_parquet(f"corpreg_nta_master.parquet")
    # 新旧データをマージして保存
    merged = merge(new, base)
    save_parquet(merged, f"corpreg_nta_master.parquet")
    # マスターデータの法人名をエンリッチ
    merged = enrich_name(merged)
    save_parquet(merged, f"corpreg_nta_master.parquet")

    # legal_form の統計情報を報告
    lf_stat = legal_form_stat(merged)
    save_parquet(lf_stat, f"legal_form_stat_{exec_date}.parquet")
    # missing_kanji に関する統計情報を報告
    mk_stat = missing_kanji_stat(merged)
    save_parquet(mk_stat, f"missing_kanji_stat_{exec_date}.parquet")