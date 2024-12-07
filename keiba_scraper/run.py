import os
import sqlite3
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import keibascraper

def setup_logger():
    """
    ロガーをセットアップする。
    ここではINFOレベルでコンソール出力を行うロガーを返す。
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def create_tables(conn, table_names, logger):
    """
    指定されたテーブル名リストに従って、テーブルを作成する。
    すでに存在する場合は何もしない。
    """
    cursor = conn.cursor()
    try:
        for table_name in table_names:
            create_table_query = keibascraper.create_table_sql(table_name)
            cursor.execute(create_table_query)
        conn.commit()
        logger.info("All specified tables are ready.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error occurred while creating tables: {e}")
        raise

def insert_data(conn, table_name, data, logger, data_id=None):
    """
    指定したテーブルにデータを挿入する。
    dataはlist of dict形式で、カラム名はdictのキーから取得する。
    data_idはログ出力用のID情報。
    """
    if not data:
        return
    cursor = conn.cursor()
    try:
        columns = ', '.join(data[0].keys())
        placeholders = ', '.join(['?'] * len(data[0]))
        insert_query = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.executemany(insert_query, [tuple(row.values()) for row in data])
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data into '{table_name}' (ID={data_id}): {e}")
        raise

def load_all_data_for_race_id(race_id, logger):
    """
    単一のrace_idに対して resultデータ、horseデータ、oddsデータをロードする。
    戻り値は (race: list[dict], result: list[dict], horse: list[dict], history: list[dict], odds: list[dict])
    horse, historyはrace_id配下の全馬に対するデータをまとめる。
    oddsは該当race_idに対するオッズデータ。
    """
    # race,result取得
    try:
        race, result = keibascraper.load("result", race_id)
    except Exception as e:
        logger.error(f"Failed to load result data for race_id={race_id}: {e}")
        return [], [], [], [], []

    # odds取得
    try:
        odds = keibascraper.load("odds", race_id)
    except Exception as e:
        logger.error(f"Failed to load odds data for race_id={race_id}: {e}")
        odds = []

    horse_data = []
    history_data = []
    for row in result:
        horse_id = row["horse_id"]
        try:
            horse, history = keibascraper.load("horse", horse_id)
            horse_data.extend(horse)
            history_data.extend(history)
        except Exception as e:
            logger.error(f"Failed to load horse data horse_id={horse_id} from race_id={race_id}: {e}")
            continue

    return race, result, horse_data, history_data, odds

def expand_race_ids(base_race_id):
    """
    入力されたbase_race_idの長さに応じてrace_idリストを展開する。
    - 12桁: そのまま1件のリスト
    - 10桁: 下2桁を01~12に展開
    - 6桁: 年月からkeibascraper.race_listを使って展開
    """
    if len(base_race_id) == 12:
        return [base_race_id]
    elif len(base_race_id) == 10:
        return [f"{base_race_id}{str(i).zfill(2)}" for i in range(1, 13)]
    elif len(base_race_id) == 6:
        year = base_race_id[:4]
        month = base_race_id[4:]
        return keibascraper.race_list(year, month)
    else:
        raise ValueError("Race ID must be 6, 10, or 12 characters long.")

def build_chunks(race_ids):
    """
    race_idsのリストをrace_id[:10]ごとにグルーピングし、dictで返す。
    key: prefix(先頭10文字)
    value: prefixを共有するrace_idのリスト
    """
    chunks = {}
    for rid in race_ids:
        prefix = rid[:10]
        if prefix not in chunks:
            chunks[prefix] = []
        chunks[prefix].append(rid)
    return chunks

def fetch_chunk_data(ids, logger):
    """
    1チャンク内の複数のrace_idについて、並列処理でデータをロードする。
    戻り値は `data_map` 辞書で、{"race": [...], "result": [...], "horse": [...], "history": [...], "odds": [...]} の形式。
    """
    data_map = {
        "race": [],
        "result": [],
        "horse": [],
        "history": [],
        "odds": []
    }

    def load_task(rid):
        return load_all_data_for_race_id(rid, logger)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(load_task, rid): rid for rid in ids}
        for future in as_completed(futures):
            rid = futures[future]
            try:
                race, result, horse_data, history_data, odds_data = future.result()
                data_map["race"].extend(race)
                data_map["result"].extend(result)
                data_map["horse"].extend(horse_data)
                data_map["history"].extend(history_data)
                data_map["odds"].extend(odds_data)
            except Exception as e:
                logger.error(f"Error in future for race_id={rid}: {e}")

    return data_map

def write_chunk_data(conn, logger, prefix, data_map):
    """
    1チャンク分のデータをDBへ書き込む処理。
    data_mapをもとに、キーがテーブル名、値がデータリストとして挿入する。
    全データ挿入後、コミットを行う。(エラー時はロールバック)
    """
    try:
        for table_name, dataset in data_map.items():
            if dataset:
                insert_data(conn, table_name, dataset, logger, data_id=prefix)
        conn.commit()
    except Exception as e:
        logger.error(f"Unexpected error inserting data for prefix={prefix}: {e}")
        conn.rollback()

def process_single_chunk(conn, logger, prefix, ids):
    """
    1チャンク(prefix)分の処理を行う。
    並列でデータを取得(fetch_chunk_data)し、その後DBへ書き込む(write_chunk_data)。
    """
    logger.info(f"Processing race ID block with prefix {prefix}")
    data_map = fetch_chunk_data(ids, logger)
    write_chunk_data(conn, logger, prefix, data_map)

def process_chunks(conn, logger, chunks):
    """
    全てのチャンクをループ処理する。
    開始時にチャンク数を表示し、各チャンクをprocess_single_chunkで処理。
    """
    logger.info(f"Total number of chunks to process: {len(chunks)}")
    for prefix, ids in chunks.items():
        process_single_chunk(conn, logger, prefix, ids)
    logger.info("All specified race IDs have been processed.")

def parse_arguments():
    """
    コマンドライン引数をパースする。
    必須引数として race_id を受け取る。
    """
    parser = argparse.ArgumentParser(description="Create tables and process keiba data.")
    parser.add_argument("race_id", type=str, help="Base Race ID (6, 10, or 12 characters)")
    return parser.parse_args()

def main():
    """
    メイン処理:
    - 引数パース
    - ロガー設定
    - DBやテーブルの初期化
    - race_ids展開
    - chunks作成
    - 全チャンク処理開始
    """
    args = parse_arguments()
    logger = setup_logger()
    db_path = "/data/keiba.sqlite"
    table_names = ["race", "horse", "history", "result", "entry", "odds"]

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)

    try:
        create_tables(conn, table_names, logger)
        race_ids = expand_race_ids(args.race_id)
        chunks = build_chunks(race_ids)
        process_chunks(conn, logger, chunks)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
