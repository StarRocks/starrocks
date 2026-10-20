---
sidebar_position: 32
displayed_sidebar: docs
description: "DBine は無料のオープンソースのデスクトップ データベース管理ツールで、MySQL プロトコルで StarRocks に接続してスキーマの参照とクエリの実行を行います。"
---

# DBine

[DBine](https://dbine.com) は、Windows、macOS、Linux 向けの無料のオープンソース（MIT ライセンス）のデスクトップ
データベース管理ツールで、130 以上のデータベースとクラウドサービスを 1 つのウィンドウで扱えます。StarRocks
用の接続タイプがあり、MySQL プロトコルで FE に接続します。

## 前提条件

- お使いのコンピュータから到達できる StarRocks クラスタと、その FE クエリポート（MySQL プロトコル、デフォルトは `9030`）。
- [リリースページ](https://github.com/addlayer-io/dbine/releases) から入手した DBine。

## 統合

1. DBine で **New connection** をクリックし、エンジンの一覧から **StarRocks** を選択します。
2. 接続設定を入力します。
   - **Server**: FE のホスト名または IP アドレス
   - **Port**: FE のクエリポート。デフォルトは `9030`
   - **Database**: 任意。最初に開くデータベース
   - **User** / **Password**: StarRocks のユーザー名とパスワード
3. **Test connection** をクリックして確認し、**Save and connect** をクリックします。

接続は **Explorer** に表示されます。次のことができます。

- データベース、テーブル、ビュー、カラム、インデックスを参照する。
- データベースを右クリックして **New query** を選び、自動補完付きのエディタで SQL を実行する。結果はグラフに
  したり、CSV、JSON、XLSX、SQL にエクスポートしたりできます。
- `EXPLAIN` によるクエリプランを表示する。
- テーブルデザイナーで、テーブルタイプ、分散方式、バケット数を指定してテーブルを作成する。
- `SHOW FULL PROCESSLIST` から、接続している FE で実行中のクエリを確認する。
- 接続を読み取り専用にして、DBine に書き込みステートメントを拒否させる。

![DBine - StarRocks に対するクエリ結果](../../_assets/IDE_dbine_1.png)
