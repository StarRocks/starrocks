---
displayed_sidebar: docs
---

# Flink Connector

## flink-connector-jdbc_2.11sink が StarRocks で 8 時間遅れている

**問題の説明:**

localtimestap 関数によって生成された時間は Flink では正常です。しかし、StarRocks に送信されたときに 8 時間遅れました。Flink サーバーと StarRocks サーバーは同じタイムゾーン、つまり Asia/Shanghai UTC/GMT+08:00 にあります。Flink のバージョンは 1.12 です。ドライバー: flink-connector-jdbc_2.11。この問題を解決する方法を教えてください。

**解決策:**

Flink のシンクテーブルで時間パラメータ 'server-time-zone' = 'Asia/Shanghai' を設定してみてください。また、jdbc URL に &serverTimezone=Asia/Shanghai を追加することもできます。以下に例を示します:

```sql
CREATE TABLE sk (
    sid int,
    local_dtm TIMESTAMP,
    curr_dtm TIMESTAMP
)
WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://192.168.110.66:9030/sys_device?characterEncoding=utf-8&serverTimezone=Asia/Shanghai',
    'table-name' = 'sink',
    'driver' = 'com.mysql.jdbc.Driver',
    'username' = 'sr',
    'password' = 'sr123',
    'server-time-zone' = 'Asia/Shanghai'
);
```

## Flink インポートでは、StarRocks クラスターにデプロイされた kafka クラスターのみをインポートできる

**問題の説明:**

```SQL
failed to query wartermark offset, err: Local: Bad message format
```

**解決策:**

Kafka の通信にはホスト名が必要です。ユーザーは StarRocks クラスターのノードでホスト名解決 /etc/hosts を設定する必要があります。

## StarRocks は 'create table statements' をバッチでエクスポートできますか？

**解決策:**

StarRocks Tools を使用してステートメントをエクスポートできます。

## BE によって要求されたメモリがオペレーションシステムに戻されない

これは通常の現象です。オペレーティングシステムからデータベースに割り当てられた大きなメモリブロックは、メモリを再利用し、メモリ割り当てをより便利にするために、割り当て時に予約され、解放時に遅延されます。ユーザーは、長期間にわたってメモリ使用量を監視することで、メモリが解放されるかどうかを確認するためにテスト環境を検証することをお勧めします。

## ダウンロード後に Flink コネクタが機能しない

**問題の説明:**

このパッケージは、Aliyun ミラーアドレスを通じて取得する必要があります。

**解決策:**

`/etc/maven/settings.xml` のミラー部分がすべて Aliyun ミラーアドレスを通じて取得されるように設定されていることを確認してください。

もしそうであれば、次のように変更してください:

<mirror>
    <id>aliyunmaven </id>
    <mirrorf>central</mirrorf>
    <name>aliyun public repo</name>
    <url>https: //maven.aliyun.com/repository/public</url>
</mirror>

## Flink-connector-StarRocks のパラメータ sink.buffer-flush.interval-ms の意味

**問題の説明:**

```plain text
+----------------------+--------------------------------------------------------------+
|         Option       | Required |  Default   | Type   |       Description           |
+-------------------------------------------------------------------------------------+
|  sink.buffer-flush.  |  NO      |   300000   | String | the flushing time interval, |
|  interval-ms         |          |            |        | range: [1000ms, 3600000ms]  |
+----------------------+--------------------------------------------------------------+
```

このパラメータが 15 秒に設定され、チェックポイント間隔が 5 分に等しい場合、この値はまだ有効ですか？

**解決策:**

3 つのしきい値のうち、どれかが最初に達成されると、そのしきい値が最初に有効になります。これは、チェックポイント間隔の値には影響されません。チェックポイント間隔の値は、exactly once にのみ機能します。interval-ms は at_least_once によって使用されます。