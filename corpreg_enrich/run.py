import sys
import pandas as pd
import os
import logging
from sudachipy import dictionary, tokenizer
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor
import re

# Base Directory
base_dir = "./data"
# Resolve Sudachi config relative to this file to avoid CWD issues
CONFIG_PATH = (Path(__file__).resolve().parent / "dict" / "sudachi.json")

# Create mode constant and a single global tokenizer with lock protection
mode = tokenizer.Tokenizer.SplitMode.C
tk = dictionary.Dictionary(config_path=str(CONFIG_PATH)).create()
tok_lock = threading.Lock()

# Set up logger object
logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
logger = logging.getLogger(__name__)

# Debug: confirm user dictionary file presence (logging must be enabled by the app)
try:
    user_dic_path = CONFIG_PATH.parent / "ja-entity-parser.dic"
    if user_dic_path.exists():
        logger.debug(f"Sudachi user dictionary: {user_dic_path} ({user_dic_path.stat().st_size} bytes)")
    else:
        logger.warning(f"Sudachi user dictionary NOT FOUND: {user_dic_path}")
except Exception as e:
    logger.debug(f"User dictionary check failed: {e}")


def make_progress_logger(total: int, step: int = 10, log_func=logging.info):
    """10%刻みなどの進捗ログを出すための簡易ロガーを作成して返す。

    戻り値: (update, done)
      - update(i): i 件目まで完了時に呼び出すと、しきい値を超えたタイミングでログ出力
      - done(): 最後に呼び出すと完了ログを出力
    """
    next_threshold = step

    def update(i: int):
        nonlocal next_threshold
        if not total:
            return
        percent = i * 100 // total
        if percent >= next_threshold:
            log_func(f"Progress: {percent}% ({i}/{total})")
            while next_threshold <= percent:
                next_threshold += step

    def done():
        if total:
            log_func("Progress: 100% (done)")

    return update, done

def load_parquet(filename: str) -> pd.DataFrame:
    """/data 配下から単一の .parquet ファイルを pandas で読み込む関数。

    - filename: /data 直下またはサブディレクトリのファイル名。拡張子がなければ自動で .parquet を付与。

    例:
        df = load_parquet_from_data("legal_form")
        df = load_parquet_from_data("corporate_registry_202508.parquet")
        df = load_parquet_from_data("subdir/file.parquet")
    """
    full_path = os.path.join(base_dir, filename + '.parquet')

    if os.path.exists(full_path):
        # 必要な列のみ読み込み
        df = pd.read_parquet(full_path, columns=["name", "furigana", "corporate_number"])
        df = df.set_index("corporate_number")
        logging.info(f"Loaded from {full_path}: {df.shape}")
        return df
    else:
        raise FileNotFoundError(f"File not found: {full_path}")


def save_csv(df: pd.DataFrame, filename: str) -> None:
    """/data 配下に単一の .csv ファイルを保存する関数。

    - filename: /data 直下またはサブディレクトリのファイル名。拡張子がなければ自動で .csv を付与。

    例:
        save_csv(df, "legal_form")
        save_csv(df, "corporate_registry_202508.csv")
        save_csv(df, "subdir/file.csv")
    """
    full_path = os.path.join(base_dir, filename + '.csv')

    df.to_csv(full_path, index=False)
    logging.info(f"Saved to {full_path}: {df.shape}")


def enrich_corporate_names(corporate_name: str) -> tuple[str, str, str]:
    """SudachiPy で社名を形態素解析し、(legal_form, brand_name) を返す。

    ルール:
      - 品詞が「名詞,普通名詞,法人,法人種別」に一致するトークンを legal_form として抽出し、
        normalized_form() を連結した文字列を返す。
      - 上記トークンを除外した残りのトークンの surface() を連結した文字列を brand_name として返す。

    引数が空や非文字列の場合は ("", "") を返す。
    """

    def is_legal_form_token(m) -> bool:
        pos = m.part_of_speech()
        # pos は [品詞1, 品詞2, 品詞3, 品詞4, 活用型, 活用形]
        return (
            isinstance(pos, (list, tuple))
            and len(pos) >= 4
            and pos[0] == "名詞"
            and pos[1] == "普通名詞"
            and pos[2] == "法人"
            and pos[3] == "法人種別"
        )

    if not isinstance(corporate_name, str) or not corporate_name:
        return "", ""

    # グローバルトークナイザをロックで保護して利用（1スレッド前提でも安全）
    with tok_lock:
        morphemes = tk.tokenize(corporate_name, mode)
    legal_tokens = [m for m in morphemes if is_legal_form_token(m)]

    # legal_form は normalized_form を入れる
    legal_form = legal_tokens[0].normalized_form() if legal_tokens else ""
    # オブジェクト同一性や __eq__ 実装依存を避けるため、品詞で再判定して除外
    brand_name = "".join(m.surface() for m in morphemes if not is_legal_form_token(m))
    furigana = "".join(m.reading_form() for m in morphemes if not is_legal_form_token(m))

    return legal_form, brand_name, furigana


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame の name_col に enrich_corporate_names を並列適用し、
    legal_form_col と brand_name_col を追加して返す。

    - df が空、または name_col が存在しない場合は空列を追加して返す。
    - デフォルトで 6 スレッドで実行。
    """
    max_workers = 1

    if "name" not in df.columns or df.empty:
        df["legal_form"] = pd.Series(index=df.index, dtype=object)
        df["brand_name"] = pd.Series(index=df.index, dtype=object)
        df["work_kana"] = pd.Series(index=df.index, dtype=object)
        return df

    names = df["name"].tolist()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        total = len(names)
        results = []
        if total > 0:
            logging.info(f"Enrichment started: {total} rows, {max_workers} threads")
        update, done = make_progress_logger(total, step=10, log_func=logging.info)
        for i, res in enumerate(executor.map(enrich_corporate_names, names), start=1):
            results.append(res)
            update(i)
        if total > 0:
            done()

    if results:
        legal_forms, brand_names, furigana = zip(*results)
        df["legal_form"] = list(legal_forms)
        df["brand_name"] = list(brand_names)
        df["work_kana"] = list(furigana)
    else:
        df["legal_form"] = pd.Series(index=df.index, dtype=object)
        df["brand_name"] = pd.Series(index=df.index, dtype=object)
        df["work_kana"] = pd.Series(index=df.index, dtype=object)

    return df

def fill_missing_furigana(df: pd.DataFrame) -> pd.DataFrame:
    """furigana 欠損値を補完する。
    1. brand_name がカタカナのみ → その値で埋める
    2. brand_name がひらがなのみ → カタカナに変換して埋める
    3. 上記以外 → work_kana で埋める（reliability=1）
    他は reliability=0
    """

    def hiragana_to_katakana(text: str) -> str:
        # ひらがな Unicode: 3041-3096, カタカナ: 30A1-30F6
        return "".join(
            chr(ord(ch) + 0x60) if "ぁ" <= ch <= "ゖ" else ch
            for ch in text
        )

    katakana_re = re.compile(r'^[\u30A0-\u30FFー]+$')
    hiragana_re = re.compile(r'^[\u3041-\u3096ー]+$')

    def fill(row):
        furigana = row.get("furigana")
        brand_name = row.get("brand_name", "")
        work_kana = row.get("work_kana", "")
        if pd.isna(furigana) or not furigana:
            if katakana_re.fullmatch(brand_name):
                reliability = 0
                value = brand_name
            elif hiragana_re.fullmatch(brand_name):
                reliability = 0
                value = hiragana_to_katakana(brand_name)
            else:
                reliability = 1
                value = work_kana
        else:
            reliability = 0
            value = furigana
        return pd.Series({"furigana": value, "reliability": reliability})

    result = df.apply(fill, axis=1)
    df["furigana"] = result["furigana"]
    df["reliability"] = result["reliability"]
    return df

if __name__ == "__main__":
    # 引数処理
    if len(sys.argv) > 1:
        input_filename = sys.argv[1]
    else:
        input_filename = "corporate_registry_202508"

    # データロード
    df_corporate = load_parquet(input_filename)
    # データをエンリッチ
    df_corporate = enrich_dataframe(df_corporate)
    df_corporate = fill_missing_furigana(df_corporate)

    # ファイルを最終化
    df_corporate.drop(columns=['work_kana'], inplace=True, errors='ignore')
    save_csv(df_corporate, f"{input_filename}_enriched")
