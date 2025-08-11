# ベースイメージ
FROM python:3.12-slim

# 作業ディレクトリを設定
WORKDIR /app

# 必要なファイルをコピー
COPY keiba_scraper /app/keiba_scraper
COPY cloud_storage /app/cloud_storage
COPY sagikoza_fetch /app/sagikoza_fetch
COPY legal_form /app/legal_form
COPY requirements.txt /app/requirements.txt

# 必要なPythonパッケージをインストール
RUN pip install --no-cache-dir -r requirements.txt

# /dataディレクトリを作成
RUN mkdir /data

# ENTRYPOINTを設定し、CMDで指定されたスクリプトを実行
ENTRYPOINT ["python", "-m"]
# CMD ["keiba_scraper.run", "202405050812"]
CMD ["legal_form.run", "corpreg_202508"]