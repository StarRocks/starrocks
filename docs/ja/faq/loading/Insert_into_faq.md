---
displayed_sidebar: docs
---

# Insert Into

## データ挿入を行う際、SQL の各挿入に 50 から 100ms かかります。効率を上げる方法はありますか？

OLAP にデータを一つずつ挿入することは推奨されません。通常はバッチで挿入されます。どちらの方法も同じ時間がかかります。

## 'Insert into select' タスクでエラーが報告される: index channel has intoleralbe failure

この問題は、Stream Load RPC のタイムアウト時間を変更することで解決できます。**be.conf** の以下の項目を変更し、変更を有効にするためにマシンを再起動してください。

`streaming_load_rpc_max_alive_time_sec`: Stream Load の RPC タイムアウト。単位: 秒。デフォルト: `1200`。

または、以下の変数を使用してクエリのタイムアウトを設定することもできます。

`query_timeout`: クエリのタイムアウト時間。単位は秒で、デフォルト値は `300` です。

## 大量のデータをロードするために INSER INTO SELECT コマンドを実行すると "execute timeout" エラーが発生する

デフォルトでは、クエリのタイムアウト時間は 300 秒です。この時間を延長するために変数 `query_timeout` を設定できます。単位は秒です。