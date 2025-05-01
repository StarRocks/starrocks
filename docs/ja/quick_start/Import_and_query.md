---
displayed_sidebar: docs
---

# データのロードとクエリ

このクイックスタートチュートリアルでは、作成したテーブルにデータをロードし（詳細は [Create a table](../quick_start/Create_table.md) を参照）、そのデータに対してクエリを実行する手順をステップバイステップで説明します。

StarRocks は、主要なクラウドサービス、ローカルファイル、ストリーミングデータシステムなど、豊富なデータソースからのデータのロードをサポートしています。詳細は [Ingestion Overview](../loading/Loading_intro.md) をご覧ください。以下の手順では、INSERT INTO ステートメントを使用して StarRocks にデータを挿入し、そのデータに対してクエリを実行する方法を示します。

> **注意**
>
> このチュートリアルは、既存の StarRocks インスタンス、データベース、テーブル、ユーザー、および独自のデータを使用して完了することができます。ただし、簡単のため、チュートリアルで提供されるスキーマとデータを使用することをお勧めします。

## ステップ 1: INSERT でデータをロード

INSERT を使用して追加のデータ行を挿入できます。詳細な手順は [INSERT](../sql-reference/sql-statements/data-manipulation/INSERT.md) を参照してください。

MySQL クライアントを介して StarRocks にログインし、作成した `sr_member` テーブルに次のデータ行を挿入するために、以下のステートメントを実行します。

```SQL
use sr_hub
INSERT INTO sr_member
WITH LABEL insertDemo
VALUES
    (001,"tom",100000,"2022-03-13",true),
    (002,"johndoe",210000,"2022-03-14",false),
    (003,"maruko",200000,"2022-03-14",true),
    (004,"ronaldo",100000,"2022-03-15",false),
    (005,"pavlov",210000,"2022-03-16",false),
    (006,"mohammed",300000,"2022-03-17",true);
```

ロードトランザクションが成功すると、次のメッセージが返されます。

```Plain
Query OK, 6 rows affected (0.07 sec)
{'label':'insertDemo', 'status':'VISIBLE', 'txnId':'5'}
```

> **注意**
>
> INSERT INTO VALUES を使用したデータのロードは、小規模なデータセットでデモを検証する必要がある場合にのみ適用されます。大規模なテストや本番環境には推奨されません。StarRocks に大量のデータをロードするには、[Ingestion Overview](../loading/Loading_intro.md) を参照して、シナリオに適した他のオプションを確認してください。

## ステップ 2: データにクエリを実行

StarRocks は SQL-92 と互換性があります。

- テーブル内のすべてのデータ行をリストする簡単なクエリを実行します。

  ```SQL
  SELECT * FROM sr_member;
  ```

  返される結果は次のとおりです。

  ```Plain
  +-------+----------+-----------+------------+----------+
  | sr_id | name     | city_code | reg_date   | verified |
  +-------+----------+-----------+------------+----------+
  |     3 | maruko   |    200000 | 2022-03-14 |        1 |
  |     1 | tom      |    100000 | 2022-03-13 |        1 |
  |     4 | ronaldo  |    100000 | 2022-03-15 |        0 |
  |     6 | mohammed |    300000 | 2022-03-17 |        1 |
  |     5 | pavlov   |    210000 | 2022-03-16 |        0 |
  |     2 | johndoe  |    210000 | 2022-03-14 |        0 |
  +-------+----------+-----------+------------+----------+
  6 rows in set (0.05 sec)
  ```

- 指定された条件で標準クエリを実行します。

  ```SQL
  SELECT sr_id, name 
  FROM sr_member
  WHERE reg_date <= "2022-03-14";
  ```

  返される結果は次のとおりです。

  ```Plain
  +-------+----------+
  | sr_id | name     |
  +-------+----------+
  |     1 | tom      |
  |     3 | maruko   |
  |     2 | johndoe  |
  +-------+----------+
  3 rows in set (0.01 sec)
  ```

- 指定されたパーティションでクエリを実行します。

  ```SQL
  SELECT sr_id, name 
  FROM sr_member 
  PARTITION (p2);
  ```

  返される結果は次のとおりです。

  ```Plain
  +-------+---------+
  | sr_id | name    |
  +-------+---------+
  |     3 | maruko  |
  |     2 | johndoe |
  +-------+---------+
  2 rows in set (0.01 sec)
  ```

## 次に何をするか

StarRocks のデータ取り込み方法について詳しくは、[Ingestion Overview](../loading/Loading_intro.md) を参照してください。StarRocks は多数の組み込み関数に加えて、[Java UDFs](../sql-reference/sql-functions/JAVA_UDF.md) もサポートしており、ビジネスシナリオに適した独自のデータ処理関数を作成できます。

また、以下の方法について学ぶことができます：

- [ETL when loading](../loading/Etl_in_loading.md) を実行する。
- 外部データソースにアクセスするための [external table](../data_source/External_table.md) を作成する。
- クエリパフォーマンスを最適化する方法を学ぶために [Analyze the query plan](../administration/Query_planning.md) を行う。