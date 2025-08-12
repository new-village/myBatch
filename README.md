# myBatch
これは作成したプロジェクトをGoogle Cloud Run 上で実行するための個人用のバッチ・フレームワークです。

## バッチ
1. legal_form  

国税庁の法人登記情報を収集して `/data/corporate_registry_<PREFECTURE>_<EXEC_DATE>.parquet` として保存し、そこに含まれる法人名から法人種別（株式会社など）とブランド名（法人種別を除く名称）、ブランド名のカタカナ名称を推定して `/data/corporate_registry_<PREFECTURE>_enriched_<EXEC_DATE>.parquet` 別データセットとして出力するバッチ。
下記コマンドで実行可能。`SHIMANE`の部分を`ALL`にすることで全国のデータを収集可能。`ALL`で実行する場合、メモリ上限を**16GB以上**に設定する必要があります。

```shell:
$ python -m legal_form.run SHIMANE
[INFO] Saved to ./data/corporate_registry_SHIMANE_20250811.parquet: (22853, 30)
[INFO] Saved to ./data/corporate_registry_SHIMANE_enriched_20250811.parquet: (22853, 5)
```

## 実行方法
1. Google Cloud のコンソールにアクセスして、Google Could Run > ジョブ を開く
2. コンテナのディプロイを選択
3. コンテナイメージのURL欄で作成されたコンテナを選択
4. ボリュームで`Cloud Storage バケット`を対象バケットには任意のバケットを選択します
5. コンテナ > ボリュームのマウントを選択して、マウント対象とするディレクトリ（通例`/data`）を指定します
6. コンテナ > 設定で コンテナの引数に`<BATCH_NAME>.run <引数>`を設定します
7. タスクのタイムアウト時間をバッチの内容に合わせて変更します
7. すぐにジョブを実行するにチェックを入れて作成します

## 新しいバッチを追加した場合
事前にGoogle Cloud BuildをトリガーしてあるためGithubにプッシュするとコンテナ・イメージが、アーティファクト・レポジトリに作成されGoogle Cloud Runで実行できるようになります。
1. `Dockerfile`に`COPY <BATCH_NAME> /app/<BATCH_NAME>`を追記
2. Google Artifact Registry のコンソールを開いて古いmybatchパッケージを削除
3. Githubに変更したレポジトリをプッシュ
4. Google Cloud Build の履歴が正常に実行完了したら準備完了

## ローカルでデバッグ
小規模のデータを置いて正常に動くことをローカルでテストすることを推奨します。
多くのバッチは`base_dir`変数をローカルの任意の場所に変更すればコマンドラインから`Python -m <BATCH_NAME>.run`で実行できます。
