---
displayed_sidebar: docs
sidebar_position: 11
---

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# SQL Plan Manager

<Beta />

このトピックでは、SQL Plan Manager 機能の基本的な概念と使用シナリオ、およびクエリプランを正規化するために SQL Plan Manager を使用する方法を紹介します。

v3.5.0 から、StarRocks は SQL Plan Manager 機能をサポートしています。

## 概要

SQL Plan Manager は、クエリプランをクエリにバインドすることを可能にし、システム状態の変化（主にデータ更新と統計更新）によるクエリプランの変更を防ぎ、クエリパフォーマンスを安定させます。

### ワークフロー

SQL Plan Manager は、事前にバインドされたクエリ SQL と使用するクエリプラン（ベースライン）を指定する必要があります。ここで、クエリとはユーザーが実行する実際のクエリ SQL を指し、クエリプランは手動で最適化されたクエリ SQL またはヒントが追加されたものを指します。

SQL Plan Manager のワークフローは次のとおりです：

1. **ベースラインの作成**: `CREATE BASELINE` コマンドを使用して、クエリプランを指定されたクエリ SQL にバインドします。
2. **クエリの書き換え**: StarRocks に送信されたクエリは、SQL Plan Manager に保存されているベースラインと自動的に照合されます。照合が成功すると、ベースラインのクエリプランがクエリに使用されます。

ベースライン作成に関する注意事項：

- バインドされた SQL とベースライン内の実行プラン SQL の論理的一貫性を確保する必要があります。StarRocks は基本的なテーブルとパラメータのチェックを行いますが、完全な論理的一貫性チェックを保証することはできません。実行プランの論理的な正確性を確保することはユーザー自身の責任です。
- デフォルトでは、ベースラインにバインドされた SQL はその SQL フィンガープリントを保存します。デフォルトでは、SQL 内の定数値は変数パラメータに置き換えられ（例えば、`t1.v1 > 1000` を `t1.v1 > ?` に変更）、SQL の一致性を向上させます。
- ベースラインにバインドされた実行プランは、SQL ロジックを変更したり、ヒント（ジョインヒントや `Set_Var`）を追加することでカスタマイズできます。
- 複雑な SQL の場合、StarRocks はベースライン内で SQL と実行プランを自動的にバインドしないことがあります。そのような場合、[高度な使用法](#advanced-usage) セクションで詳述されているように手動でバインドを行うことができます。

クエリの書き換えに関する注意事項：

- SQL Plan Manager は主に SQL フィンガープリントの一致に依存しています。クエリの SQL フィンガープリントがベースラインのものと一致するかどうかを確認します。クエリがベースラインと一致する場合、クエリ内のパラメータは自動的にベースラインの実行プランに置き換えられます。
- 照合プロセス中に、ステータスが `enable` の複数のベースラインとクエリが一致した場合、オプティマイザは最適なベースラインを評価して選択します。
- 照合プロセス中に、SQL Plan Manager はベースラインとクエリが一致するかどうかを検証します。一致に失敗した場合、ベースラインのクエリプランは使用されません。
- SQL Plan Manager によって書き換えられた実行プランの場合、`EXPLAIN` ステートメントは `Using baseline plan[id]` を返します。

## ベースラインの管理

### ベースラインの作成

**構文**:

```SQL
CREATE [GLOBAL] BASELINE [ON <BindSQL>] USING <PlanSQL> 
[PROPERTIES ("key" = "value"[, ...])]
```

**パラメータ**:

- `GLOBAL`: （オプション）グローバルレベルのベースラインを作成します。
- `BindSQL`: （オプション）ベースライン（実行プラン）クエリにバインドされる特定のクエリ。このパラメータが指定されていない場合、ベースラインクエリは自分自身にバインドされ、自分のクエリプランを使用します。
- `PlanSQL`: 実行プランを定義するために使用されるクエリ。

**例**:

```SQL
-- セッションレベルの BASELINE を作成し、ベースライン SQL を直接自分自身にバインドし、自分のクエリプランを使用します。
CREATE BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- グローバルレベルの BASELINE を作成し、ベースライン SQL を直接自分自身にバインドし、指定されたジョインヒントを使用して自分のクエリプランを使用します。
CREATE GLOBAL BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- セッションレベルの BASELINE を作成し、クエリをベースライン SQL にバインドし、指定されたジョインヒントを使用してベースライン SQL クエリプランを使用します。
CREATE BASELINE ON SELECT t1.v2, t2.v3 FROM t1, t2 WHERE t1.v2 = t2.v2 AND t1.v2 > 100
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 on t1.v2 = t2.v2 where t1.v2 > 100;
```

### ベースラインの表示

**構文**:

```SQL
SHOW BASELINE [WHERE <condition>]

SHOW BASELINE [ON <query>]
```

**例**:

```Plain
MySQL > show baseline\G;
***************************[ 1. row ]***************************
Id            | 646125
global        | N
enable        | N
bindSQLDigest | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` LIMIT 2
bindSQLHash   | 1085294
bindSQL       | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` LIMIT 2
planSQL       | SELECT t2.v1 AS c_1, t2.v2 AS c_2, t2.v3 AS c_3, t1.v1 AS c_4, t1.v2 AS c_5 FROM t2 INNER JOIN[SHUFFLE] t1 ON t2.v2 = t1.v2 LIMIT 2
costs         | 582.0
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 14:50:45
***************************[ 2. row ]***************************
Id            | 636134
global        | Y
enable        | Y
bindSQLDigest | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = ?
bindSQLHash   | 1085294
bindSQL       | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = _spm_const_range(1, 10, 20)
planSQL       | SELECT t_0.v1 AS c_1, t_0.v2 AS c_2, t_0.v3 AS c_3, t1.v1 AS c_4, t1.v2 AS c_5 FROM (SELECT * FROM t2 WHERE v3 = _spm_const_range(1, 10, 20)) t_0 INNER JOIN[SHUFFLE] t1 ON t_0.v2 = t1.v2
costs         | 551.0204081632653
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-13 15:29:04
2 rows in set
Time: 0.019s

MySQL > show baseline where global = true\G;
***************************[ 1. row ]***************************
Id            | 636134
global        | Y
enable        | Y
bindSQLDigest | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = ?
bindSQLHash   | 1085294
bindSQL       | SELECT * FROM `td`.`t2` INNER JOIN `td`.`t1` ON `td`.`t2`.`v2` = `td`.`t1`.`v2` WHERE `td`.`t2`.`v3` = _spm_const_range(1, 10, 20)
planSQL       | SELECT t_0.v1 AS c_1, t_0.v2 AS c_2, t_0.v3 AS c_3, t1.v1 AS c_4, t1.v2 AS c_5 FROM (SELECT * FROM t2 WHERE v3 = _spm_const_range(1, 10, 20)) t_0 INNER JOIN[SHUFFLE] t1 ON t_0.v2 = t1.v2
costs         | 551.0204081632653
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-13 15:29:04
1 row in set
Time: 0.013s

MySQL > show baseline on SELECT count(1) AS `count(1)` FROM `old`.`t1` INNER JOIN `old`.`t2` ON `old`.`t1`.`k2` = `old`.`t2`.`k2` LIMIT 10\G;
***************************[ 1. row ]***************************
Id            | 679817
global        | Y
enable        | Y
bindSQLDigest | SELECT count(?) AS `count(1)` FROM `old`.`t1` INNER JOIN `old`.`t2` ON `old`.`t1`.`k2` = `old`.`t2`.`k2` LIMIT 10
bindSQLHash   | 1085927
bindSQL       | SELECT count(_spm_const_var(1)) AS `count(1)` FROM `old`.`t1` INNER JOIN `old`.`t2` ON `old`.`t1`.`k2` = `old`.`t2`.`k2` LIMIT 10
planSQL       | SELECT count(_spm_const_var(1)) AS c_7 FROM (SELECT 1 AS c_9 FROM t1 INNER JOIN[SHUFFLE] t2 ON t1.k2 = t2.k2) t_0 LIMIT 10
costs         | 2532.6
queryMs       | 35.0
source        | CAPTURE
updateTime    | 2025-05-27 11:17:48
1 row in set
Time: 0.026s
```

### ベースラインの削除

**構文**:

```SQL
DROP BASELINE <id>,<id>...
```

**パラメータ**:

`id`: ベースラインの ID。`SHOW BASELINE` を実行してベースライン ID を取得で���ます。

**例**:

```SQL
-- ID が 140035 のベースラインを削除します。
DROP BASELINE 140035;
```

### ベースラインの有効化/無効化

**構文**:

```SQL
ENABLE BASELINE <id>,<id>...
DISABLE BASELINE <id>,<id>...
```

**パラメータ**:

`id`: ベースラインの ID。`SHOW BASELINE` を実行してベースライン ID を取得できます。

**例**:

```SQL
-- ID が 140035 のベースラインを有効化します。
ENABLE BASELINE 140035;
-- ID が 140035 と 140037 のベースラインを無効化します。
DISABLE BASELINE 140035, 140037;
```

## クエリの書き換え

SQL Plan Manager のクエリの書き換え機能を有効にするには、変数 `enable_spm_rewrite` を `true` に設定します。

```SQL
SET enable_spm_rewrite = true;
```

実行プランをバインドした後、StarRocks は対応するクエリを自動的に対応するクエリプランに書き換えます。

**例**:

元の SQL と実行プランを確認します：

```Plain
mysql> EXPLAIN SELECT t1.v2, t2.v3 FROM t1, t2 WHERE t1.v2 = t2.v2 AND t1.v1 > 20;
+-----------------------------------------+
| Explain String                          |
+-----------------------------------------+
| PLAN FRAGMENT 0                         |
|  OUTPUT EXPRS:2: v2 | 5: v3             |
|   PARTITION: UNPARTITIONED              |
......
|   5:HASH JOIN                           |
|   |  join op: INNER JOIN (PARTITIONED)  |
|   |  colocate: false, reason:           |
|   |  equal join conjunct: 4: v2 = 2: v2 |
|   |                                     |
|   |----4:EXCHANGE                       |
|   |                                     |
|   1:EXCHANGE                            |
......
```

ジョインヒントを使用して元の SQL を SQL 実行プランにバインドするベースラインを作成します：

```SQL
MySQL td> create global baseline on select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 1000
using select t1.v2, t2.v3 from t1 join[broadcast] t2 on t1.v2 = t2.v2 where t1.v1 > 1000;
Query OK, 0 rows affected
Time: 0.074s
MySQL td> show baseline\G;
***************************[ 1. row ]***************************
Id            | 647139
global        | Y
enable        | Y
bindSQLDigest | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3` FROM `td`.`t1` , `td`.`t2`  WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > ?)
bindSQLHash   | 1085294
bindSQL       | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3` FROM `td`.`t1` , `td`.`t2`  WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > _spm_const_var(1))
planSQL       | SELECT t_0.v2 AS c_2, t2.v3 AS c_5 FROM (SELECT v2 FROM t1 WHERE v1 > _spm_const_var(1)) t_0 INNER JOIN[BROADCAST] t2 ON t_0.v2 = t2.v2
costs         | 1193.0
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 15:51:36
1 rows in set
Time: 0.016s
```

SQL Plan Manager のクエリの書き換えを有効にし、元のクエリがベースラインによって書き換えられるかどうかを確認します：

```Plain
MySQL td> show variables like '%enable_spm_re%'
+--------------------+-------+
| Variable_name      | Value |
+--------------------+-------+
| enable_spm_rewrite | false |
+--------------------+-------+
1 row in set
Time: 0.007s
MySQL td> set enable_spm_rewrite=true
Query OK, 0 rows affected
Time: 0.001s
MySQL td> explain select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 20;
+-----------------------------------------+
| Explain String                          |
+-----------------------------------------+
| Using baseline plan[647139]             |
|                                         |
| PLAN FRAGMENT 0                         |
|  OUTPUT EXPRS:2: v2 | 5: v3             |
.............
|   4:HASH JOIN                           |
|   |  join op: INNER JOIN (BROADCAST)    |
|   |  colocate: false, reason:           |
|   |  equal join conjunct: 2: v2 = 4: v2 |
|   |                                     |
|   |----3:EXCHANGE                       |
|   |                                     |
|   1:Project                             |
|   |  <slot 2> : 2: v2                   |
.............
```

## 高度な使用法

次のシナリオでは、手動でクエリプランのバインドを試みることができます：
- 複雑な SQL の場合、SQL Plan Manager は SQL とクエリプランを自動的にバインドできません。
- 特定のシナリオ（例えば、固定パラメータや条件付きパラメータ）では、自動バインドが要件を満たせません。

自動バインドと比較して、手動バインドはより柔軟性がありますが、SQL Plan Manager の実行メカニズムを理解する必要があります。

### 実行ロジック

#### SQL Plan Manager 関数

SQL Plan Manager 関数は、SQL Plan Manager 内のプレースホルダー関数であり、主に次の目的があります：
- パラメータ化された SQL 内の式をマークし、後続のパラメータ抽出とプロセス内での置換のために使用します。
- パラメータ条件をチェックし、異なるパラメータを持つ SQL をパラメータ条件を通じて異なるクエリプランにマッピングします。

現在、StarRocks は次の SQL Plan Manager 関数をサポートしています：
- `_spm_const_var(placeholdID)`: 単一の定数値をマークするために使用されます。
- `_spm_const_list(placeholdID)`: 複数の定数値をマークするために使用され、通常は IN 条件内の複数の定数値をマークするために使用されます。
- `_spm_const_range(placeholdID, min, max)`: 単一の定数値をマークするために使用されますが、定数値が指定された範囲 '[min, max]' 内にある必要があります。
- `_spm_const_num(placeholdID, value...)`: 単一の定数値をマークするために使用されますが、定数値が指定された列挙 'value...' の値である必要があります。

'placeholdID' は整数であり、パラメータの一意の識別子として使用され、ベースラインをバインドし、プランを生成する際に使用されます。

#### ベースライン作成プロセス

1. `CREATE BASELINE` を実行して、バインドされる `BindSQL` と実行プランクエリ `PlanSQL` を取得します。
2. `BindSQL` をパラメータ化します：リテラル値または式を SQL Plan Manager 関数で置き換えます。例えば、`id > 200` を `id > _spm_const_var(0)` に置き換えます。ここでパラメータ `0` は `BindSQL` と `PlanSQL` 内の式の位置を確認するために使用されるプレースホルダー ID です。詳細な SQL Plan Manager 関数については、[SQL Plan Manager 関数](#sql-plan-manager-functions) を参照してください。
3. `PlanSQL` 内のプレースホルダーをバインドします：`PlanSQL` 内のプレースホルダーの位置を特定し、それらを元の式で置き換えます。
4. オプティマイザを使用して `PlanSQL` を最適化し、クエリプランを取得します。
5. クエリプランをヒント付きの SQL にシリアライズします。
6. ベースライン（BindSQL の SQL フィンガープリント、最適化された実行プラン SQL）を保存します。

#### クエリ書き換えプロセス

クエリの書き換えロジックは `PREPARE` ステートメントに似ています。
1. クエリを実行します。
2. クエリを SQL フィンガープリントに正規化します。
3. SQL フィンガープリントを使用してベースラインを見つけます（ベースラインの `BindSQL` と一致させます）。
4. クエリをベースラインにバインドし、クエリがベースラインの `BindSQL` と一致するかどうかを確認し、`BindSQL` 内の SQL Plan Manager 関数を使用してクエリから対応するパラメータ値を抽出します。例えば、クエリ内の `id > 1000` は BindSQL 内の `id > _spm_const_var(0)` にバインドされ、`_spm_const_var(0) = 1000` を抽出します。
5. ベースラインの `PlanSQL` 内の SQL Plan Manager パラメータを置き換えます。
6. `PlanSQL` を返して元のクエリを置き換えます。

### クエリを手動でバインドする

SQL Plan Manager 関数を使用して、より複雑な SQL をベースラインにバインドできます。

#### 例 1
例えば、バインドする SQL は次のとおりです：

```SQL
create global baseline using
with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color in ('slate', 'blanched', 'burnished') and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color in ('slate', 'blanched', 'burnished') and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
```

`i_color in ('slate', 'blanched', 'burnished')` の定数値が同じであるため、SQL は SQL Plan Manager によって次のように認識されます：

```SQL
with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color IN (_spm_const_list(1)) and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color IN (_spm_const_list(1)) and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
```

つまり、両方の `i_color in ('xxx', 'xxx')` インスタンスが同じパラメータとして認識され、SQL Plan Manager が異なるパラメータを使用する SQL を区別することができなくなります。

```SQL
-- ベースラインをバインドできます
MySQL tpcds> explain with ss as ( 
    select i_item_id, sum(ss_ext_sales_price) total_sales 
    from store_sales, item 
    where i_color in ('A', 'B', 'C') and ss_item_sk = i_item_sk 
    group by i_item_id 
), 
cs as ( 
    select i_item_id, sum(cs_ext_sales_price) total_sales 
    from catalog_sales, item 
    where i_color in ('A', 'B', 'C') and cs_item_sk = i_item_sk 
    group by i_item_id 
) 
select i_item_id, sum(total_sales) total_sales 
from (  select * from ss 
        union all 
        select * from cs) tmp1 
group by i_item_id; 
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| Using baseline plan[646215]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
................................                                                            |
|      avgRowSize=3.0                                                                       |
+-------------------------------------------------------------------------------------------+
184 rows in set
Time: 0.095s

-- ベースラインをバインドできません
MySQL tpcds> explain with ss as ( 
    select i_item_id, sum(ss_ext_sales_price) total_sales 
    from store_sales, item 
    where i_color in ('A', 'B', 'C') and ss_item_sk = i_item_sk 
    group by i_item_id 
), 
cs as ( 
    select i_item_id, sum(cs_ext_sales_price) total_sales 
    from catalog_sales, item 
    where i_color in ('E', 'F', 'G') and cs_item_sk = i_item_sk 
    group by i_item_id 
) 
select i_item_id, sum(total_sales) total_sales 
from (  select * from ss 
        union all 
        select * from cs) tmp1 
group by i_item_id; 
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
................................                                                            |
|      avgRowSize=3.0                                                                       |
+-------------------------------------------------------------------------------------------+
182 rows in set
Time: 0.040s
```

このような場合、`BindSQL` と `PlanSQL` に手動でパラメータを指定できます：
```SQL
create global baseline using
with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color IN (_spm_const_list(1)) and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color IN (_spm_const_list(2)) and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
```

クエリがベースラインによって書き換えられることを確認します：

```Plain
MySQL td> show baseline\G;
***************************[ 1. row ]***************************
Id            | 646215
global        | Y
enable        | Y
bindSQLDigest | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
bindSQLHash   | 203487418
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
planSQL       | SELECT c_104, sum(c_105) AS c_106 FROM (SELECT * FROM (SELECT i_item_id AS c_104, c_46 AS c_105 FROM (SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(1))) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id) t_2 UNION ALL SELECT i_item_id AS c_104, c_103 AS c_105 FROM (SELECT i_item_id, sum(cs_ext_sales_price) AS c_103 FROM (SELECT i_item_id, cs_ext_sales_price FROM catalog_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(1))) t_3 ON cs_item_sk = i_item_sk) t_4 GROUP BY i_item_id) t_5) t_6) t_7 GROUP BY c_104
costs         | 2.608997082E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 15:30:29
***************************[ 2. row ]***************************
Id            | 646237
global        | Y
enable        | Y
bindSQLDigest | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (?)) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
bindSQLHash   | 203487418
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales` FROM `tpcds`.`catalog_sales` , `tpcds`.`item`  WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(2))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`) GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales` FROM (SELECT * FROM `ss` UNION ALL SELECT * FROM `cs`) `tmp1` GROUP BY `tmp1`.`i_item_id`
planSQL       | SELECT c_104, sum(c_105) AS c_106 FROM (SELECT * FROM (SELECT i_item_id AS c_104, c_46 AS c_105 FROM (SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(1))) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id) t_2 UNION ALL SELECT i_item_id AS c_104, c_103 AS c_105 FROM (SELECT i_item_id, sum(cs_ext_sales_price) AS c_103 FROM (SELECT i_item_id, cs_ext_sales_price FROM catalog_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color IN (_spm_const_list(2))) t_3 ON cs_item_sk = i_item_sk) t_4 GROUP BY i_item_id) t_5) t_6) t_7 GROUP BY c_104
costs         | 2.635637082E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 15:37:35
2 rows in set
Time: 0.013s
MySQL td> explain with ss as ( 
    select i_item_id, sum(ss_ext_sales_price) total_sales 
    from store_sales, item 
    where i_color in ('A', 'B', 'C') and ss_item_sk = i_item_sk 
    group by i_item_id 
), 
cs as ( 
    select i_item_id, sum(cs_ext_sales_price) total_sales 
    from catalog_sales, item 
    where i_color in ('E', 'F', 'G') and cs_item_sk = i_item_sk 
    group by i_item_id 
) 
select i_item_id, sum(total_sales) total_sales 
from (  select * from ss 
        union all 
        select * from cs) tmp1 
group by i_item_id; 
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| Using baseline plan[646237]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
......
```

上記の出力から、SQL Plan Manager 関数で区別されたパラメータを持つベースラインによってクエリが書き換えられていることがわかります。

#### 例 2

次のクエリの場合、異なる `i_color` に対して異なるベースラインを使用したい場合：
```SQL
select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk
where i_color = 25 
group by i_item_id
```

'_spm_const_range' を使用して：
```SQL
-- 10 <= i_color <= 50 の場合、SHUFFLE JOIN を使用
MySQL tpcds> create baseline  using select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join[SHUFFLE] item on ss_item_sk = i_item_sk
where i_color = _spm_const_range(1, 10, 50)
group by i_item_id
Query OK, 0 rows affected
Time: 0.017s
-- i_color が 60、70、80 の場合、BROADCAST JOIN を使用
MySQL tpcds> create baseline  using select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join[BROADCAST] item on ss_item_sk = i_item_sk
where i_color = _spm_const_enum(1, 60, 70, 80)
group by i_item_id
Query OK, 0 rows affected
Time: 0.009s
MySQL tpcds> show baseline\G;
***************************[ 1. row ]***************************
Id            | 647167
global        | N
enable        | Y
bindSQLDigest | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = ? GROUP BY `tpcds`.`item`.`i_item_id`
bindSQLHash   | 68196091
bindSQL       | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = _spm_const_range(1, 10, 50) GROUP BY `tpcds`.`item`.`i_item_id`
planSQL       | SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[SHUFFLE] (SELECT i_item_sk, i_item_id FROM item WHERE i_color = _spm_const_range(1, '10', '50')) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id
costs         | 1.612502146E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 16:02:46
***************************[ 2. row ]***************************
Id            | 647171
global        | N
enable        | Y
bindSQLDigest | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = ? GROUP BY `tpcds`.`item`.`i_item_id`
bindSQLHash   | 68196091
bindSQL       | SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales` FROM `tpcds`.`store_sales` INNER JOIN `tpcds`.`item` ON `tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk` WHERE `tpcds`.`item`.`i_color` = _spm_const_enum(1, 60, 70, 80) GROUP BY `tpcds`.`item`.`i_item_id`
planSQL       | SELECT i_item_id, sum(ss_ext_sales_price) AS c_46 FROM (SELECT i_item_id, ss_ext_sales_price FROM store_sales INNER JOIN[BROADCAST] (SELECT i_item_sk, i_item_id FROM item WHERE i_color = _spm_const_enum(1, '60', '70', '80')) t_0 ON ss_item_sk = i_item_sk) t_1 GROUP BY i_item_id
costs         | 1.457490986E8
queryMs       | -1.0
source        | USER
updateTime    | 2025-05-16 16:03:23
2 rows in set
Time: 0.011s
MySQL tpcds>
MySQL tpcds> explain select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk 
where i_color = 40 -- SHUFFLE JOIN にヒット
group by i_item_id
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| Using baseline plan[647167]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
.................
|   |                                                                                       |
|   5:HASH JOIN                                                                             |
|   |  join op: INNER JOIN (PARTITIONED)                                                    |
|   |  colocate: false, reason:                                                             |
|   |  equal join conjunct: 2: ss_item_sk = 24: i_item_sk                                   |
|   |                                                                                       |
|   |----4:EXCHANGE                                                                         |
|   |                                                                                       |
|   1:EXCHANGE                                                                              |
.................
MySQL tpcds> explain select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk
where i_color = 70 -- BROADCAST JOIN にヒット
group by i_item_id
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| Using baseline plan[647171]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
.................
|   4:HASH JOIN                                                                             |
|   |  join op: INNER JOIN (BROADCAST)                                                      |
|   |  colocate: false, reason:                                                             |
|   |  equal join conjunct: 2: ss_item_sk = 24: i_item_sk                                   |
|   |                                                                                       |
|   |----3:EXCHANGE                                                                         |
|   |                                                                                       |
|   0:OlapScanNode                                                                          |
|      TABLE: store_sales                                                                   |
.................
MySQL tpcds> explain select i_item_id, sum(ss_ext_sales_price) total_sales
from store_sales join item on ss_item_sk = i_item_sk 
where i_color = 100  -- ベースラインを使用しない
group by i_item_id
+-------------------------------------------------------------------------------------------+
| Explain String                                                                            |
+-------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:25: i_item_id | 46: sum                                                     |
|   PARTITION: UNPARTITIONED                                                                |
|                                                                                           |
|   RESULT SINK                                                                             |
.................
```

### 自動キャプチャ

自動キャプチャは、過去の期間（デフォルトで3時間）におけるクエリ SQL ステートメントをクエリし、これらのクエリに基づいてベースラインを生成して保存します。生成されたベースラインはデフ���ルトで 'disable' 状態であり、すぐには有効になりません。
次のシナリオで：
* アップグレード後、実行プランが変更され、クエリ時間が長くなる
* データが変更され、統計が変更され、実行プランが変更される

`show baseline` を使用して履歴ベースラインを見つけ、`enable baseline` を使用してプランを手動でロールバックできます。

自動キャプチャ機能は、クエリ履歴保存機能に依存しており、次の設定が必要です：
```SQL
set global enable_query_history=true;
```
クエリ履歴は '_statistics_.query_history' テーブルに保存されます。

自動キャプチャを有効にするには：
```SQL
set global enable_plan_capture=true;
```

その他の設定：
```SQL
-- クエリ履歴の保存期間、単位：秒、デフォルトは3日
set global query_history_keep_seconds = 259200;
-- 自動キャプチャの作業間隔、単位：秒、デフォルトは3時間
set global plan_capture_interval=10800;
-- SQL テーブルの定期的なチェックをキャプチャし、plan_capture_include_pattern に一致するテーブル名（db.table）の場合にのみ SQL をキャプチャします。デフォルトは .* で、すべてのテーブルを表します。
set global plan_capture_include_pattern=".*";
```

:::note
1. クエリ履歴の保存と自動キャプチャは、いくつかのストレージと計算リソースを消費するため、自分のシナリオに応じて適切に設定してください。
2. ベースラインをバインドした後、アップグレード後に新しく追加された最適化が無効になる可能性があるため、自動キャプチャされたベースラインはデフォルトで無効になっています。
:::

#### 使用例

アップグレード時に自動キャプチャ機能を使用して、アップグレード後のプラン回帰問題を回避する：

1. アップグレードの1〜2日前に自動キャプチャ機能を有効にする：
```SQL
set global enable_query_history=true;
set global enable_plan_capture=true;
```

2. StarRocks が定期的にクエリプランを記録し始め、`show baseline` で確認できます

3. StarRocks をアップグレードする

4. アップグレード後、クエリ実行時間をチェックするか、次の SQL を使用してプラン変更があったクエリを特定する：
```SQL
WITH recent_queries AS (
    -- 3日以内のクエリ実行時間を平均実行時間として使用
    SELECT 
        dt,                     -- クエリ実行時間
        sql_digest,             -- クエリの SQL フィンガープリント
        `sql`,                  -- クエリ SQL
        query_ms,               -- 実行時間
        plan,                   -- 使用されたクエリプラン
        AVG(query_ms) OVER (PARTITION BY sql_digest) AS avg_ms, -- SQL フィンガープリントグループ内の平均実行時間
        RANK() OVER (PARTITION BY sql_digest ORDER BY plan) != 1 AS is_changed -- 異なるプラン形式を変更指標として数える
    FROM _statistics_.query_history
    WHERE dt >= NOW() - INTERVAL 3 DAY
)
-- 過去12時間で平均の1.5倍を超える実行時間のクエリ
SELECT *, RANK() OVER (PARTITION BY sql_digest ORDER BY query_ms DESC) AS rnk
FROM recent_queries
WHERE query_ms > avg_ms * 1.5 and dt >= now() - INTERVAL 12 HOUR
```

5. プラン変更情報またはクエリ実行時間情報に基づいて、プランロールバックが必要かどうかを判断する

6. SQL と時点に対応するベースラインを見つける：
```SQL
show baseline on <query>
```

7. Enable baseline を使用してロールバックする：
```SQL
enable baseline <id>
```

8. ベースライン書き換えスイッチを有効にする：
```SQL
set enable_spm_rewrite = true;
```

## 将来の計画

将来的には、StarRocks は SQL Plan Manager に基づいたより高度な機能を提供する予定です。これには以下が含まれます：
- SQL プランの安定性チェックの強化。
- 固定クエリプランの自動最適化。
- より多くの条件付きパラメータバインディング方法のサポート。