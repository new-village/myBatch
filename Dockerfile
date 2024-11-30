# ベースイメージ
FROM python:3.12-slim

# 作業ディレクトリを設定
WORKDIR /app

# 必要なファイルをコピー
COPY batch_scripts/ /app/batch_scripts/
COPY requirements.txt /app/requirements.txt

# 必要なPythonパッケージをインストール
RUN pip install --no-cache-dir -r requirements.txt

# /dataディレクトリを作成
RUN mkdir /data/race
RUN mkdir /data/horse

# ENTRYPOINTを設定し、CMDで指定されたスクリプトを実行
ENTRYPOINT ["python", "-m"]
CMD ["batch_scripts.keibascraper", "2024050508"]