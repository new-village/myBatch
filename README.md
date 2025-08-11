# myBatch
これは作成したプロジェクトをGoogle Cloud Run 上で実行するための個人用のバッチ・フレームワークです。

## 実行方法
1. Google Cloud のコンソールにアクセスして、Google Could Run > ジョブ を開く
2. コンテナのディプロイを選択
3. コンテナイメージのURL欄で作成されたコンテナを選択
4. ボリュームで`Cloud Storage バケット`を対象バケットには任意のバケットを選択します
5. コンテナでマウント対象とするディレクトリ（通例`/data`）を指定します
6. コンテナの編集 > 設定で コンテナの引数に`<BATCH_NAME>.run <引数>`を設定します
7. すぐにジョブを実行するにチェックを入れて作成します

## 新しいバッチを追加した場合
事前にGoogle Cloud BuildをトリガーしてあるためGithubにプッシュするとコンテナ・イメージが、アーティファクト・レポジトリに作成されGoogle Cloud Runで実行できるようになります。
1. `Dockerfile`に`COPY <BATCH_NAME> /app/<BATCH_NAME>`を追記
2. Google Artifact Registry のコンソールを開いて古いmybatchパッケージを削除
3. Githubに変更したレポジトリをプッシュ
4. Google Cloud Build の履歴が正常に実行完了したら準備完了
