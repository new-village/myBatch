import os
import re
import sys
import logging
import pandas as pd
import jpcorpreg
import duckdb
from ja_entityparser import corporate_parser

# Set up logger object
logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
logger = logging.getLogger(__name__)

con = duckdb.connect()
con.execute("PRAGMA threads=2")
con.execute("PRAGMA memory_limit='3GB'")  # コンテナ4GB運用でも安全

def fetch(filename:str, prefecture:str = "ALL") -> None:
    """法人番号公表サイトから法人情報を取得する関数

    例:
        df = fetch("corpreg_nta_202508.parquet", "ALL")
    """
    # 法人情報を取得して保存
    file_path = jpcorpreg.load(prefecture=prefecture, format="parquet")
    con.read_parquet(file_path).write_parquet(filename, compression="zstd")

    cnt = con.read_parquet(new_file).aggregate("COUNT(*)").fetchone()[0]
    logger.info(f"Fetch {filename}: {cnt}")
    return

def merge(new_file: str, base_file: str) -> None:
    """新しい法人情報と古い法人情報をマージする関数

    例:
        merged = merge(new, old)
    """
    # base_file が無い場合は new_file をそのまま初期化として書き出し
    if not os.path.exists(base_file):
        select = """
            *,
            CAST(NULL AS VARCHAR) AS legal_form,
            CAST(NULL AS VARCHAR) AS brand_name,
            CAST(NULL AS VARCHAR) AS brand_kana,
            CAST(0 AS INTEGER) AS reliability
        """
        con.read_parquet(new_file).project(select).write_parquet(base_file, compression="zstd")
        cnt = con.read_parquet(new_file).aggregate("COUNT(*)").fetchone()[0]
        logger.info(f"Create {base_file}: {cnt}")
        return

    # new/base を一時ビューとして登録
    con.read_parquet(new_file).create_view("new_data", replace=True)
    con.read_parquet(base_file).create_view("base_data", replace=True)

    # base に存在しない行を抽出（ビュー名に予約語を避ける）
    con.sql(f"""
        CREATE OR REPLACE TEMP VIEW unique_new AS
        SELECT
            n.*,
            CAST(NULL AS VARCHAR) AS legal_form,
            CAST(NULL AS VARCHAR) AS brand_name,
            CAST(NULL AS VARCHAR) AS brand_kana,
            CAST(0 AS INTEGER) AS reliability
        FROM new_data n
        ANTI JOIN base_data b ON 
            n.corporate_number = b.corporate_number AND
            n.update_date      = b.update_date;
    """)

    # UNIONして一時ファイルに書き出して原子的置換
    tmp_out = base_file + ".tmp"
    con.sql("SELECT * FROM base_data UNION ALL SELECT * FROM unique_new") \
      .write_parquet(tmp_out, compression="zstd")
    os.replace(tmp_out, base_file)

    # 件数検算
    cnt = con.read_parquet(base_file).aggregate("COUNT(*)").fetchone()[0]
    logger.info(f"Add new/change records to {base_file}: {cnt}")
    return

def _parse_tuple(name: str) -> tuple[str, str, str]:
    """ ユーザ定義関数で法人名を解析し、法人種別、ブランド名、ブランド名カナを返す
    """
    d = corporate_parser(name)
    return (d.get("legal_form"), d.get("brand_name"), d.get("brand_kana"))

con.create_function(
    "parse",
    _parse_tuple,
    ["VARCHAR"],  # 引数型
    "STRUCT(legal_form VARCHAR, brand_name VARCHAR, brand_kana VARCHAR)"  # 戻り値
)

def enrich_name(base_file: str) -> None:
    """法人種別、ブランド名、ブランド名カナを補完する関数
    例:
        enrich_name("corporate_registry.parquet")
    """
    # 一時ファイルに書き出し
    tmp_out = base_file + ".tmp"
    select = """
        * REPLACE (
            (parse(name)).legal_form::VARCHAR AS legal_form,
            (parse(name)).brand_name::VARCHAR AS brand_name,
            (parse(name)).brand_kana::VARCHAR AS brand_kana
        )
    """
    con.read_parquet(base_file).project(select).write_parquet(tmp_out, compression="zstd")
    os.replace(tmp_out, base_file)

    # 件数検算
    cnt, lf_cnt = con.read_parquet(base_file).filter("kind not in ('101','201','399','499') AND latest = '1'").aggregate("COUNT(*), COUNT(legal_form)").fetchone()
    logger.info(f"Complete enrich_name: {lf_cnt}/{cnt}({lf_cnt/cnt*100:.2f}%)")
    return

def _is_katakana(kana: str) -> bool:
    """ カタカナかどうかを判定する関数
    """
    reg = re.compile(r'^[\u30A0-\u30FF\uFF65-\uFF9F\u3000\s]+$')
    return bool(reg.fullmatch(kana))

con.create_function(
    "is_katakana",
    _is_katakana,
    ["VARCHAR"],  # 引数型
    "BOOLEAN"  # 戻り値
)

def enrich_furigana(base_file: str) -> None:
    """フリガナを補完する関数
    例:
        enrich_furigana("corporate_registry.parquet")
    """
    # 一時ファイルに書き出し
    tmp_out = base_file + ".tmp"

    # furiganaが空白/欠損ならfuriganaにbrand_kanaを代入
    select = """
        * REPLACE (
            CASE WHEN furigana IS NOT NULL THEN furigana ELSE brand_kana END AS brand_kana,
            CASE WHEN furigana IS NOT NULL OR is_katakana(brand_name) THEN 0 ELSE 1 END AS reliability
        )
    """
    con.read_parquet(base_file).project(select).write_parquet(tmp_out, compression="zstd")
    os.replace(tmp_out, base_file)

    # 件数検算
    cnt, rel_cnt = con.read_parquet(base_file).aggregate("COUNT(*), SUM(reliability)").fetchone()
    logger.info(f"Complete enrich_furigana: {rel_cnt}/{cnt}({rel_cnt/cnt*100:.2f}%)")
    return

def stat_report(base_file: str) -> None:
    """統計情報の報告を行う関数
    例:
        stat_report(df)
    """
    # 総件数の取得
    total = con.read_parquet(base_file).aggregate("COUNT(*)").fetchone()[0]

    # Name に'＿'（外字）が含まれるレコードの抽出
    cond = "name like '%＿%'"
    select = "corporate_number, name, name_image_id"
    filename = os.path.join("/data", "irregular_name.parquet")
    con.read_parquet(base_file).filter(cond).project(select).write_parquet(filename, compression="zstd")
    count = con.read_parquet(filename).aggregate("COUNT(*)").fetchone()[0]
    logger.info(f"Save {filename}: {count}/{total}({count/total*100:.2f}%)")

    # Legal form のパース失敗レコードの抽出
    cond = "kind not in ('101','201','399','499') AND latest = '1'"
    lf_total = con.read_parquet(base_file).filter(cond).aggregate("COUNT(*)").fetchone()[0]
    cond = cond + " AND legal_form IS NULL"
    select = "corporate_number, name, legal_form, brand_name"
    filename = os.path.join("/data", "irregular_legal_form.parquet")
    con.read_parquet(base_file).filter(cond).project(select).write_parquet(filename, compression="zstd")
    count = con.read_parquet(filename).aggregate("COUNT(*)").fetchone()[0]
    logger.info(f"Save {filename}: {count}/{lf_total}({count/lf_total*100:.2f}%)")

    # フリガナが不十分なレコードの抽出
    cond = "reliability = 1"
    select = "corporate_number, name, brand_name, furigana, brand_kana"
    filename = os.path.join("/data", "irregular_furigana.parquet")
    con.read_parquet(base_file).filter(cond).project(select).write_parquet(filename, compression="zstd")
    count = con.read_parquet(filename).aggregate("COUNT(*)").fetchone()[0]
    logger.info(f"Save {filename}: {count}/{total}({count/total*100:.2f}%)")
    

if __name__ == "__main__":
    exec_date = pd.Timestamp.now().strftime("%Y%m%d")

    # 引数処理
    if len(sys.argv) > 1:
        prefecture = sys.argv[1].upper()
    else:
        prefecture = "ALL"

    # 法人番号サイトからデータを取得して保存
    new_file = os.path.join("/data", f"corpreg_nta_{exec_date[:6]}.parquet")
    fetch(new_file, prefecture)

    # 新旧データをマージして保存
    base_file = os.path.join("/data", f"corpreg_nta_master.parquet")
    merge(new_file, base_file)

    # 名称のエンリッチ
    enrich_name(base_file)
    enrich_furigana(base_file)

    # 統計レポート
    stat_report(base_file)