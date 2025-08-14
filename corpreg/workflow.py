import os
import sys
import logging
import pandas as pd
import jpcorpreg
from ja_entityparser import corporate_parser
from concurrent.futures import ProcessPoolExecutor

# Set up logger object
logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
logger = logging.getLogger(__name__)


def load_parquet(filename: str) -> pd.DataFrame:
    """/data 配下に単一の .parquet ファイルを読み込む関数。

    - filename: /data 直下またはサブディレクトリのファイル名。

    例:
        df = load_parquet("corporate_registry_202508.parquet")
        df = load_parquet("subdir/file.parquet")
    """
    full_path = os.path.join("/data", filename)
    if os.path.exists(full_path):
        df = pd.read_parquet(full_path)
        logger.info(f"Loaded from {full_path}: {df.shape}")
    else:
        df = pd.DataFrame(columns=["corporate_number", "name"])
        logger.warning(f"File not found: {full_path}. Returning empty DataFrame.")
    return df

def save_parquet(df: pd.DataFrame, filename: str) -> None:
    """/data 配下に単一の .parquet ファイルを保存する関数。

    - filename: /data 直下またはサブディレクトリのファイル名。

    例:
        save_parquet(df, "corporate_registry_202508.parquet")
        save_parquet(df, "subdir/file.parquet")
    """
    full_path = os.path.join("/data", filename)
    df.to_parquet(full_path, index=True)
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
    df = jpcorpreg.load(prefecture=prefecture)

    if df is not None:
        save_parquet(df, f"raw_corpreg_{exec_date}.parquet")
    else:
        df = pd.DataFrame(index=pd.Index([], name="corporate_number"))
        logger.warning(f"Corporate registry data do not found. Returning empty DataFrame.")
    
    return df

def merge(new: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    """新しい法人情報と古い法人情報をマージする関数

    例:
        merged = merge(new, old)
    """
    if base.empty:
        logger.warning("Skip merging with empty base DataFrame.")
        return new

    # 新旧のデータフレームを単純に縦結合して、重複を削除
    merged = pd.concat([new, base], axis=0)
    merged.drop_duplicates(keep="first", inplace=True)

    # 全行の latest=0 で初期化
    merged["latest"] = 0
    # corporate_number ごとに update_date が最大の行を latest=1 に設定
    is_latest = merged["update_date"].eq(
        merged.groupby("corporate_number")["update_date"].transform("max")
    )
    merged.loc[is_latest, "latest"] = 1

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
        }
    except Exception:
        # 例外は親に伝播させず欠損扱い
        return {"legal_form": None, "brand_name": None}

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
    # 法人番号サイトからデータを取得して保存
    new = fetch()
    # 前回実行分のデータを読み込み
    base = load_parquet(f"nta_corporate_registry.parquet")
    # 新旧データをマージして保存
    merged = merge(new, base)
    save_parquet(merged, f"nta_corporate_registry.parquet")
    # マスターデータの法人名をエンリッチ
    merged = enrich_name(merged)
    save_parquet(merged, f"nta_corporate_registry.parquet")
    
    exec_date = pd.Timestamp.now().strftime("%Y%m%d")
    # legal_form の統計情報を報告
    lf_stat = legal_form_stat(merged)
    save_parquet(lf_stat, f"legal_form_stat_{exec_date}.parquet")
    # missing_kanji に関する統計情報を報告
    mk_stat = missing_kanji_stat(merged)
    save_parquet(mk_stat, f"missing_kanji_stat_{exec_date}.parquet")