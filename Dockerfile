# ベースイメージ
FROM python:3.12-slim

# 作業ディレクトリを設定
WORKDIR /app

# 必要なファイルをコピー
COPY keiba_scraper /app/keiba_scraper
COPY cloud_storage /app/cloud_storage
COPY sagikoza_fetch /app/sagikoza_fetch
COPY corpreg /app/corpreg
COPY requirements.txt /app/requirements.txt
COPY main.py /app/main.py

# 必要なPythonパッケージをインストール
RUN pip install --no-cache-dir -r requirements.txt

# /dataディレクトリを作成
RUN mkdir /data

# ENTRYPOINTを設定
ENTRYPOINT ["python", "main.py"]