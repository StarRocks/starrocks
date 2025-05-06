---
displayed_sidebar: docs
---

# DataX writer

## Introduction

StarRocksWriter プラグインは、StarRocks の宛先テーブルにデータを書き込むことを可能にします。具体的には、StarRocksWriter は [Stream Load](../../loading/StreamLoad.md) を介して CSV または JSON 形式でデータを StarRocks にインポートし、内部でキャッシュし、`reader` によって読み取られたデータを StarRocks に一括でインポートして、より良い書き込みパフォーマンスを実現します。全体のデータフローは `source -> Reader -> DataX channel -> Writer -> StarRocks` です。

[Download the plugin](https://github.com/StarRocks/DataX/releases)

DataX のフルパッケージをダウンロードするには `https://github.com/alibaba/DataX` にアクセスし、starrockswriter プラグインを `datax/plugin/writer/` ディレクトリに配置してください。

以下のコマンドを使用してテストします:
`python datax.py --jvm="-Xms6G -Xmx6G" --loglevel=debug job.json`

## Function Description

### Sample Configuration

ここでは、MySQL からデータを読み取り、StarRocks にロードするための設定ファイルを示します。

```json
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "column": [ "k1", "k2", "v1", "v2" ],
                        "connection": [
                            {
                                "table": [ "table1", "table2" ],
                                "jdbcUrl": [
                                     "jdbc:mysql://127.0.0.1:3306/datax_test1"
                                ]
                            },
                            {
                                "table": [ "table3", "table4" ],
                                "jdbcUrl": [
                                     "jdbc:mysql://127.0.0.1:3306/datax_test2"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "starrockswriter",
                    "parameter": {
                        "username": "xxxx",
                        "password": "xxxx",
                        "database": "xxxx",
                        "table": "xxxx",
                        "column": ["k1", "k2", "v1", "v2"],
                        "preSql": [],
                        "postSql": [], 
                        "jdbcUrl": "jdbc:mysql://172.28.17.100:9030/",
                        "loadUrl": ["172.28.17.100:8030", "172.28.17.100:8030"],
                        "loadProps": {}
                    }
                }
            }
        ]
    }
}

```

## Starrockswriter Parameter Description

* **username**

  * 説明: StarRocks データベースのユーザー名

  * 必須: はい

  * デフォルト値: なし

* **password**

  * 説明: StarRocks データベースのパスワード

  * 必須: はい

  * デフォルト: なし

* **database**

  * 説明: StarRocks テーブルのデータベース名

  * 必須: はい

  * デフォルト: なし

* **table**

  * 説明: StarRocks テーブルのテーブル名

  * 必須: はい

  * デフォルト: なし

* **loadUrl**

  * 説明: stream load のための StarRocks FE のアドレス。複数の FE アドレスを指定可能で、形式は `fe_ip:fe_http_port` です。

  * 必須: はい

  * デフォルト値: なし

* **column**

  * 説明: データを書き込む宛先テーブルのフィールド。カラムはカンマで区切ります。例: "column": ["id", "name", "age"]。
    >**column 設定項目は指定する必要があり、空白にすることはできません。**
    >注意: 宛先テーブルのカラム数、タイプなどを変更した場合、ジョブが正しく動作しないか失敗する可能性があるため、空白にしないことを強くお勧めします。設定項目は reader の querySQL または column と同じ順序である必要があります。

* 必須: はい

* デフォルト値: なし

* **preSql**

* 説明: データを書き込む前に宛先テーブルで実行される標準ステートメント。

* 必須: いいえ

* デフォルト: なし

* **jdbcUrl**

  * 説明: `preSql` および `postSql` を実行するための宛先データベースの JDBC 接続情報。
  
  * 必須: いいえ

* デフォルト: なし

* **loadProps**

  * 説明: StreamLoad のリクエストパラメータ。詳細は StreamLoad の紹介ページを参照してください。

  * 必須: いいえ

  * デフォルト値: なし

## Type conversion

デフォルトでは、入力データは文字列に変換され、`t` がカラムセパレータ、`n` が行セパレータとして使用され、StreamLoad インポート用の `csv` ファイルを形成します。
カラムセパレータを変更するには、`loadProps` を適切に設定してください。

```json
"loadProps": {
    "column_separator": "\\x01",
    "row_delimiter": "\\x02" 
}
```

インポート形式を `json` に変更するには、`loadProps` を適切に設定してください。

```json
"loadProps": {
    "format": "json",
    "strip_outer_array": true
}
```

> `json` 形式は、ライターがデータを JSON 形式で StarRocks にインポートするためのものです。

## About time zone

ソース tp ライブラリが別のタイムゾーンにある場合、datax.py を実行する際に、コマンドラインの後に以下のパラメータを追加します。

```json
"-Duser.timezone=xx"
```

例: DataX が Postgrest データをインポートし、ソースライブラリが UTC タイムにある場合、起動時にパラメータ "-Duser.timezone=GMT+0" を追加します。