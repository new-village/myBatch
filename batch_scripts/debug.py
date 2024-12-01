import os
import logging
from datetime import datetime

# ログ設定
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("debug.log"),  # ログをファイルにも出力
        logging.StreamHandler()           # 標準出力にも出力
    ]
)

def ensure_directory_exists(directory_path):
    """ディレクトリが存在するか確認し、なければ作成"""
    logging.debug(f"Checking if directory exists: {directory_path}")
    if not os.path.exists(directory_path):
        logging.info(f"Directory does not exist. Creating: {directory_path}")
        try:
            os.makedirs(directory_path)
        except Exception as e:
            logging.error(f"Failed to create directory: {e}")
            raise
    else:
        logging.debug(f"Directory already exists: {directory_path}")

def create_empty_file(file_path):
    """空のファイルを作成"""
    logging.debug(f"Attempting to create empty file: {file_path}")
    try:
        with open(file_path, 'w') as f:
            pass  # 空のファイルを作成
        logging.info(f"Empty file created successfully: {file_path}")
    except Exception as e:
        logging.error(f"Failed to create file: {e}")
        raise

def main():
    # ターゲットディレクトリとファイル名
    target_directory = "/data/horse/"
    file_name = f"empty_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    file_path = os.path.join(target_directory, file_name)

    logging.info("Starting script execution...")

    # ディレクトリチェック＆作成
    try:
        ensure_directory_exists(target_directory)
    except Exception as e:
        logging.critical(f"Could not ensure directory exists: {e}")
        return

    # 空のファイルを作成
    try:
        create_empty_file(file_path)
    except Exception as e:
        logging.critical(f"Could not create file: {e}")
        return

    logging.info("Script executed successfully.")

if __name__ == "__main__":
    main()