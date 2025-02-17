---
displayed_sidebar: docs
sidebar_position: 10
---

# テーブル概要

import Replicanum from '../_assets/commonMarkdown/replicanum.md'

テーブルはデータストレージの単位です。StarRocksにおけるテーブル構造を理解し、効率的なテーブル構造を設計することは、データの組織化を最適化し、クエリ効率を向上させるのに役立ちます。また、従来のデータベースと比較して、StarRocksはJSONやARRAYのような複雑な半構造化データをカラム形式で保存し、クエリパフォーマンスを向上させることができます。

このトピックでは、StarRocksにおけるテーブル構造を基本的かつ一般的な視点から紹介します。

v3.3.1以降、StarRocksはDefault Catalogで一時テーブルの作成をサポートしています。

## 基本的なテーブル構造の始め方

他のリレーショナルデータベースと同様に、テーブルは論理的に行と列で構成されています。

- 行: 各行はレコードを保持します。各行には関連するデータ値のセットが含まれています。
- 列: 列は各レコードの属性を定義します。各列は特定の属性のデータを保持します。例えば、従業員テーブルには、名前、従業員ID、部署、給与などの列が含まれる場合があり、各列は対応するデータを保存します。各列のデータは同じデータ型です。テーブル内のすべての行は同じ数の列を持ちます。

StarRocksでテーブルを作成するのは簡単です。CREATE TABLE文で列とそのデータ型を定義するだけでテーブルを作成できます。例:

```SQL
CREATE DATABASE example_db;
USE example_db;
CREATE TABLE user_access (
    uid int,
    name varchar(64),
    age int, 
    phone varchar(16),
    last_access datetime,
    credits double
)
ORDER BY (uid, name);
```

上記のCREATE TABLEの例は、重複キーテーブルを作成します。このタイプのテーブルには列に制約が追加されないため、重複したデータ行がテーブル内に存在することができます。重複キーテーブルの最初の2つの列はソート列として指定され、ソートキーを形成します。データはソートキーに基づいてソートされた後に保存され、クエリ中のインデックス作成を加速することができます。

v3.3.0以降、重複キーテーブルは`ORDER BY`を使用してソートキーを指定することをサポートしています。`ORDER BY`と`DUPLICATE KEY`の両方が使用される場合、`DUPLICATE KEY`は効果を発揮しません。

<Replicanum />

[DESCRIBE](../sql-reference/sql-statements/table_bucket_part_index/DESCRIBE.md)を実行してテーブルスキーマを表示します。

```SQL
MySQL [test]> DESCRIBE user_access;
+-------------+-------------+------+-------+---------+-------+
| Field       | Type        | Null | Key   | Default | Extra |
+-------------+-------------+------+-------+---------+-------+
| uid         | int         | YES  | true  | NULL    |       |
| name        | varchar(64) | YES  | true  | NULL    |       |
| age         | int         | YES  | false | NULL    |       |
| phone       | varchar(16) | YES  | false | NULL    |       |
| last_access | datetime    | YES  | false | NULL    |       |
| credits     | double      | YES  | false | NULL    |       |
+-------------+-------------+------+-------+---------+-------+
6 rows in set (0.00 sec)
```

[SHOW CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md)を実行してCREATE TABLE文を表示します。

```SQL
MySQL [example_db]> SHOW CREATE TABLE user_access\G
*************************** 1. row ***************************
       Table: user_access
Create Table: CREATE TABLE `user_access` (
  `uid` int(11) NULL COMMENT "",
  `name` varchar(64) NULL COMMENT "",
  `age` int(11) NULL COMMENT "",
  `phone` varchar(16) NULL COMMENT "",
  `last_access` datetime NULL COMMENT "",
  `credits` double NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`uid`, `name`)
DISTRIBUTED BY RANDOM
ORDER BY(`uid`, `name`)
PROPERTIES (
"bucket_size" = "4294967296",
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "3"
);
1 row in set (0.01 sec)
```

## 総合的なテーブル構造を理解する

StarRocksのテーブル構造を深く理解することで、ビジネスニーズに合わせた効率的なデータ管理構造を設計することができます。

### [テーブルタイプ](./table_types/table_types.md)

StarRocksは、重複キーテーブル、主キーテーブル、集計テーブル、ユニークキーテーブルの4種類のテーブルを提供し、生データ、頻繁に更新されるリアルタイムデータ、集計データなど、さまざまなビジネスシナリオに対応したデータを保存します。

- 重複キーテーブルはシンプルで使いやすいです。このタイプのテーブルには列に制約が追加されないため、重複したデータ行がテーブル内に存在することができます。重複キーテーブルは、制約や事前集計を必要としないログなどの生データを保存するのに適しています。
- 主キーテーブルは強力です。主キー列には一意性と非NULL制約が追加されます。主キーテーブルはリアルタイムの頻繁な更新と部分的な列の更新をサポートし、高いクエリパフォーマンスを確保するため、リアルタイムクエリシナリオに適しています。
- 集計テーブルは事前集計されたデータを保存するのに適しており、スキャンおよび計算されるデータ量を削減し、集計クエリの効率を向上させます。
- ユニークテーブルも頻繁に更新されるリアルタイムデータを保存するのに適していますが、このタイプのテーブルはより強力な主キーテーブルに置き換えられつつあります。

### [データ分散](data_distribution/Data_distribution.md)

StarRocksは、パーティション化+バケッティングの2層データ分散戦略を使用して、データをBEsに均等に分散します。よく設計されたデータ分散戦略は、スキャンされるデータ量を効果的に削減し、StarRocksの同時処理能力を最大化し、クエリパフォーマンスを向上させます。

![img](../_assets/table_design/table_overview.png)

#### パーティション化

最初のレベルはパーティション化です。テーブル内のデータは、通常、日付や時間を保持するパーティション列に基づいて小さなデータ管理単位に分割できます。クエリ中に、パーティションプルーニングはスキャンする必要のあるデータ量を削減し、クエリパフォーマンスを効果的に最適化します。

StarRocksは、使いやすいパーティション化の手法である式に基づくパーティション化を提供し、さらに柔軟な方法として範囲やリストパーティション化も提供しています。

#### バケッティング

2番目のレベルはバケッティングです。パーティション内のデータは、さらにバケッティングを通じて小さなデータ管理単位に分割されます。各バケットのレプリカはBEsに均等に分散され、高いデータ可用性を確保します。

StarRocksは2つのバケッティング方法を提供しています。

- ハッシュバケッティング: バケッティングキーのハッシュ値に基づいてデータをバケットに分配します。クエリで条件列として頻繁に使用される列をバケッティング列として選択することで、クエリ効率を向上させることができます。
- ランダムバケッティング: データをランダムにバケットに分配します。このバケッティング方法はよりシンプルで使いやすいです。

### [データ型](../sql-reference/data-types/README.md)

NUMERIC、DATE、STRINGなどの基本データ型に加えて、StarRocksはARRAY、JSON、MAP、STRUCTなどの複雑な半構造化データ型をサポートしています。

### [インデックス](indexes/indexes.md)

インデックスは特別なデータ構造であり、テーブル内のデータへのポインタとして使用されます。クエリの条件列がインデックス化された列である場合、StarRocksは条件を満たすデータを迅速に特定できます。

StarRocksは組み込みのインデックスを提供しています。プレフィックスインデックス、オーディナルインデックス、ゾーンマップインデックスです。また、ユーザーがインデックスを作成することも可能で、ビットマップインデックスやブルームフィルターインデックスを使用してクエリ効率をさらに向上させることができます。

### 制約

制約はデータの整合性、一貫性、正確性を確保するのに役立ちます。主キーテーブルの主キー列は一意でNULLでない値を持たなければなりません。集計テーブルの集計キー列とユニークキーテーブルのユニークキー列は一意の値を持たなければなりません。

### 一時テーブル

データを処理する際、将来の再利用のために中間結果を保存する必要があるかもしれません。初期バージョンでは、StarRocksは単一のクエリ内で一時的な結果を定義するためにCTE（Common Table Expressions）の使用のみをサポートしていました。しかし、CTEは単なる論理構造であり、結果を物理的に保存せず、異なるクエリ間で使用することができないため、一定の制限があります。中間結果を保存するためにテーブルを作成することを選択した場合、これらのテーブルのライフサイクルを管理する必要があり、コストがかかります。

この問題に対処するために、StarRocksはv3.3.1で一時テーブルを導入しました。一時テーブルは、ETLプロセスからの中間結果などのデータを一時的にテーブルに保存することを可能にし、そのライフサイクルはセッションに結びつけられ、StarRocksによって管理されます。セッションが終了すると、一時テーブルは自動的にクリアされます。一時テーブルは現在のセッション内でのみ表示され、異なるセッションで同じ名前の一時テーブルを作成することができます。

#### 使用法

以下のSQL文で`TEMPORARY`キーワードを使用して一時テーブルを作成および削除できます。

- [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)
- [CREATE TABLE AS SELECT](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_AS_SELECT.md)
- [CREATE TABLE LIKE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE_LIKE.md)
- [DROP TABLE](../sql-reference/sql-statements/table_bucket_part_index/DROP_TABLE.md)

:::note

他のタイプの内部テーブルと同様に、一時テーブルはDefault Catalogの下のデータベースで作成する必要があります。しかし、一時テーブルはセッションベースであるため、一意の名前制約を受けません。異なるセッションで同じ名前の一時テーブルを作成することができ、さらには他の非一時的な内部テーブルと同じ名前の一時テーブルを作成することもできます。

データベース内に同じ名前の一時テーブルと非一時テーブルがある場合、一時テーブルが優先されます。セッション内では、同じ名前のテーブルに対するすべてのクエリと操作は一時テーブルにのみ影響を与えます。

:::

#### 制限事項

一時テーブルの使用法は内部テーブルと似ていますが、いくつかの制約と違いがあります。

- 一時テーブルはDefault Catalogで作成する必要があります。
- コロケートグループの設定はサポートされていません。テーブル作成時に`colocate_with`プロパティが明示的に指定された場合、それは無視されます。
- テーブル作成時に`ENGINE`は`olap`として指定する必要があります。
- ALTER TABLE文はサポートされていません。
- 一時テーブルに基づくビューおよびマテリアライズドビューの作成はサポートされていません。
- EXPORT文はサポートされていません。
- SELECT INTO OUTFILE文はサポートされていません。
- 一時テーブルを作成するための非同期タスクのSUBMIT TASKはサポートされていません。

### その他の機能

上記の機能に加えて、ビジネス要件に基づいてより堅牢なテーブル構造を設計するために、さらに多くの機能を採用することができます。例えば、ビットマップやHLL列を使用して重複排除カウントを加速したり、生成列やオートインクリメント列を指定して一部のクエリを高速化したり、柔軟で自動的なストレージクールダウン方法を設定してメンテナンスコストを削減したり、Colocate Joinを設定してマルチテーブルJOINクエリを高速化することができます。詳細については、[CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)を参照してください。