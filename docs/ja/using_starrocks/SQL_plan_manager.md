---
displayed_sidebar: docs
sidebar_position: 11
---

# [プレビュー] SQL Plan Manager

このトピックでは、SQL Plan Manager 機能の基本概念と使用シナリオ、およびクエリプランを正規化するために SQL Plan Manager を使用する方法を紹介します。

v3.5.0 から、StarRocks は SQL Plan Manager 機能をサポートしています。

## 概要

SQL Plan Manager は、クエリプランをクエリにバインドすることで、システム状態の変化（主にデータ更新と統計更新）によるクエリプランの変更を防ぎ、クエリパフォーマンスを安定させます。

### ワークフロー

SQL Plan Manager では、使用するバインドされたクエリ SQL とクエリプラン（ベースライン）を事前に指定する必要があります。ここで、クエリはユーザーが実行する実際のクエリ SQL を指し、クエリプランは手動で最適化されたクエリ SQL またはヒントが追加されたものを指します。

SQL Plan Manager のワークフローは次のとおりです：

1. **ベースラインの作成**: `CREATE BASELINE` コマンドを使用して、指定されたクエリ SQL にクエリプランをバインドします。
2. **クエリの書き換え**: StarRocks に送信されたクエリは、SQL Plan Manager に保存されているベースラインと自動的に一致します。成功すると、ベースラインのクエリプランがクエリに使用されます。

ベースライン作成の注意点：

- バインドされた SQL とベースラインの実行プラン SQL の間の論理的一貫性を確保する必要があります。StarRocks は基本的なテーブルとパラメータのチェックを行いますが、完全な論理的一貫性チェックを保証することはできません。実行プランの論理的な正確性を確保するのはユーザーの責任です。
- デフォルトでは、ベースラインにバインドされた SQL はその SQL フィンガープリントを保存します。デフォルトでは、SQL 内の定数値は変数パラメータに置き換えられ（例：`t1.v1 > 1000` を `t1.v1 > ?` に変更）、SQL の一致を改善します。
- ベースラインにバインドされた実行プランは、SQL ロジックを変更したり、ヒント（ジョインヒントや `Set_Var`）を追加することでカスタマイズできます。
- 複雑な SQL の場合、StarRocks はベースラインで SQL と実行プランを自動的にバインドできないことがあります。その場合、[高度な使用法](#高度な使用法)セクションで詳述されているように、手動でバインドを使用できます。

クエリの書き換えに関する注意点：

- SQL Plan Manager は主に SQL フィンガープリントの一致に依存しています。クエリの SQL フィンガープリントがベースラインのものと一致するかどうかを確認します。クエリがベースラインと一致する場合、クエリ内のパラメータは自動的にベースラインの実行プランに置き換えられます。
- 一致プロセス中に、クエリが複数のベースラインと一致する場合、オプティマイザは最適なベースラインを評価して選択します。
- 一致プロセス中に、SQL Plan Manager はベースラインとクエリが一致するかどうかを検証します。一致に失敗した場合、ベースラインのクエリプランは使用されません。
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
- `BindSQL`: （オプション）ベースライン（実行プラン）クエリにバインドする特定のクエリ。このパラメータが指定されていない場合、ベースラインクエリは自身にバインドされ、自身のクエリプランを使用します。
- `PlanSQL`: 実行を定義するために使用されるクエリ。

**例**:

```SQL
-- セッションレベルの BASELINE を作成し、ベースライン SQL を直接自身にバインドし、自身のクエリプランを使用します。
CREATE BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- グローバルレベルの BASELINE を作成し、ベースライン SQL を直接自身にバインドし、指定されたジョインヒントを使用して自身のクエリプランを使用します。
CREATE GLOBAL BASELINE
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 ON t1.v2 = t2.v2 WHERE t1.v2 > 100;

-- セッションレベルの BASELINE を作成し、クエリをベースライン SQL にバインドし、指定されたジョインヒントを使用してベースライン SQL クエリプランを使用します。
CREATE BASELINE ON SELECT t1.v2, t2.v3 FROM t1, t2 WHERE t1.v2 = t2.v2 AND t1.v2 > 100
USING SELECT t1.v2, t2.v3 FROM t1 JOIN[BROADCAST] t2 on t1.v2 = t2.v2 where t1.v2 > 100;
```

### ベースラインの表示

**構文**:

```SQL
SHOW BASELINE
```

**例**:

```Plain
mysql> SHOW BASELINE\G;
***************************[ 1. row ]***************************
Id            | 435269
global        | Y
bindSQLDigest | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3`
FROM `td`.`t1` , `td`.`t2` 
WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > ?)
bindSQLHash   | 1085294
bindSQL       | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3`
FROM `td`.`t1` , `td`.`t2` 
WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > _spm_const_var(1))
planSQL       | SELECT c_2, c_5 FROM (SELECT t_0.v2 AS c_2, t2.v3 AS c_5 FROM (SELECT v2 FROM t1 WHERE v1 > _spm_const_var(1)) t_0 INNER JOIN[BROADCAST] t2 ON t_0.v2 = t2.v2) t2
costs         | 263.0
updateTime    | 2025-03-10 16:01:50
1 row in set
Time: 0.013s
```

### ベースラインの削除

**構文**:

```SQL
DROP BASELINE <id>
```

**パラメータ**:

`id`: ベースラインの ID。`SHOW BASELINE` を実行してベースライン ID を取得できます。

**例**:

```SQL
-- ID が 140035 のベースラインを削除します。
DROP BASELINE 140035;
```

## クエリの書き換え

SQL Plan Manager のクエリの書き換え機能を有効にするには、変数 `enable_sql_plan_manager_rewrite` を `true` に設定します。

```SQL
SET enable_sql_plan_manager_rewrite = true;
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
|                                         |
|   RESULT SINK                           |
|                                         |
|   7:EXCHANGE                            |
|                                         |
| PLAN FRAGMENT 1                         |
|  OUTPUT EXPRS:                          |
|   PARTITION: HASH_PARTITIONED: 4: v2    |
|                                         |
|   STREAM DATA SINK                      |
|     EXCHANGE ID: 07                     |
|     UNPARTITIONED                       |
|                                         |
|   6:Project                             |
|   |  <slot 2> : 2: v2                   |
|   |  <slot 5> : 5: v3                   |
|   |                                     |
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

ジョインヒントを使用して元の SQL を SQL 実行プランにバインドするためのベースラインを作成します：

```SQL
mysql> create global baseline on select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 1000
using select t1.v2, t2.v3 from t1 join[broadcast] t2 on t1.v2 = t2.v2 where t1.v1 > 1000;
Query OK, 0 rows affected
Time: 0.062s
mysql> show baseline\G;
***************************[ 1. row ]***************************
Id            | 435269
global        | Y
bindSQLDigest | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3`
FROM `td`.`t1` , `td`.`t2` 
WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > ?)
bindSQLHash   | 1085294
bindSQL       | SELECT `td`.`t1`.`v2`, `td`.`t2`.`v3`
FROM `td`.`t1` , `td`.`t2` 
WHERE (`td`.`t1`.`v2` = `td`.`t2`.`v2`) AND (`td`.`t1`.`v1` > _spm_const_var(1))
planSQL       | SELECT c_2, c_5 FROM (SELECT t_0.v2 AS c_2, t2.v3 AS c_5 FROM (SELECT v2 FROM t1 WHERE v1 > _spm_const_var(1)) t_0 INNER JOIN[BROADCAST] t2 ON t_0.v2 = t2.v2) t2
costs         | 263.0
updateTime    | 2025-03-10 16:01:50
1 row in set
Time: 0.013s
```

SQL Plan Manager のクエリの書き換えを有効にし、元のクエリがベースラインによって書き換えられているかどうかを確認します：

```Plain
mysql> show session variables like "%enable_sql_plan_manager_rewrite%";
+---------------------------------+-------+
| Variable_name                   | Value |
+---------------------------------+-------+
| enable_sql_plan_manager_rewrite | false |
+---------------------------------+-------+
1 row in set
Time: 0.006s
mysql> set enable_sql_plan_manager_rewrite = true;
Query OK, 0 rows affected
Time: 0.002s
mysql> explain select t1.v2, t2.v3 from t1, t2 where t1.v2 = t2.v2 and t1.v1 > 20;
+-----------------------------------------+
| Explain String                          |
+-----------------------------------------+
| Using baseline plan[435269]             |
|                                         |
| PLAN FRAGMENT 0                         |
|  OUTPUT EXPRS:2: v2 | 5: v3             |
|   PARTITION: UNPARTITIONED              |
|                                         |
|   RESULT SINK                           |
|                                         |
|   6:EXCHANGE                            |
|                                         |
| PLAN FRAGMENT 1                         |
|  OUTPUT EXPRS:                          |
|   PARTITION: RANDOM                     |
|                                         |
|   STREAM DATA SINK                      |
|     EXCHANGE ID: 06                     |
|     UNPARTITIONED                       |
|                                         |
|   5:Project                             |
|   |  <slot 2> : 2: v2                   |
|   |  <slot 5> : 5: v3                   |
|   |                                     |
|   4:HASH JOIN                           |
|   |  join op: INNER JOIN (BROADCAST)    |
|   |  colocate: false, reason:           |
|   |  equal join conjunct: 2: v2 = 4: v2 |
|   |                                     |
|   |----3:EXCHANGE                       |
|   |                                     |
|   1:Project                             |
|   |  <slot 2> : 2: v2                   |
|   |                                     |
|   0:OlapScanNode                        |
|      TABLE: t1                          |
.......
```

## 高度な使用法

次のシナリオでは、手動でクエリプランのバインドを試すことができます：
- 複雑な SQL の場合、SQL Plan Manager は SQL とクエリプランを自動的にバインドできません。
- 特定のシナリオ（例：固定パラメータや条件付きパラメータ）では、自動バインドが要件を満たせないことがあります。

自動バインドと比較して、手動バインドはより柔軟性がありますが、SQL Plan Manager の実行メカニズムを理解する必要があります。

### 実行ロジック

#### ベースライン作成プロセス

1. `CREATE BASELINE` を実行して、バインドされる `BindSQL` と実行プランクエリ `PlanSQL` を取得します。
2. `BindSQL` をパラメータ化します：リテラル値や式を SQL Plan Manager 関数で置き換えます。たとえば、`id > 200` を `id > _spm_const_var(0)` に置き換え、パラメータ `0` は `BindSQL` と `PlanSQL` の式の位置を確認するためのプレースホルダー ID として使用されます。その他の SQL Plan Manager 関数については、[SQL Plan Manager Functions](#sql-plan-manager-関数) を参照してください。
3. `PlanSQL` のプレースホルダーをバインドします：`PlanSQL` 内のプレースホルダーの位置を特定し、それを元の式に置き換えます。
4. オプティマイザを使用して `PlanSQL` を最適化し、クエリプランを取得します。
5. クエリプランをヒント付きの SQL にシリアライズします。
6. ベースラインを保存します（BindSQL の SQL フィンガープリント、最適化された実行プラン SQL）。

#### クエリの書き換えプロセス

クエリの書き換えロジックは `PREPARE` ステートメントに似ています。
1. クエリを実行します。
2. クエリを SQL フィンガープリントに正規化します。
3. SQL フィンガープリントを使用してベースラインを見つけます（ベースラインの `BindSQL` と一致させます）。
4. クエリをベースラインにバインドし、クエリがベースラインの `BindSQL` と一致するかどうかを確認し、`BindSQL` の SQL Plan Manager 関数を使用してクエリから対応するパラメータ値を抽出します。たとえば、クエリの `id > 1000` は BindSQL の `id > _spm_const_var(0)` にバインドされ、`_spm_const_var(0) = 1000` を抽出します。
5. ベースラインの `PlanSQL` 内の SQL Plan Manager パラメータを置き換えます。
6. `PlanSQL` を返して元のクエリを置き換えます。

### SQL Plan Manager 関数

SQL Plan Manager 関数は、SQL Plan Manager 内のプレースホルダー関数であり、主に次の目的があります：
- パラメータ化された SQL 内の式をマークし、プロセス中のパラメータ抽出と置き換えを行います。
- パラメータ条件をチェックし、異なるパラメータを持つ SQL を異なるクエリプランにマッピングします。

現在、StarRocks は次の SQL Plan Manager 関数をサポートしています：
- `_spm_const_var`: 単一の定数値をマークするために使用されます。
- `_spm_const_list`: 複数の定数値をマークするために使用され、通常は IN 条件内の複数の定数値をマークするために使用されます。

将来的には、条件付きパラメータを持つプレースホルダー関数を提供するために、新しい SQL Plan Manager 関数 `_spm_const_range` や `_spm_const_enum` がサポートされる予定です。

### クエリを手動でバインドする

SQL Plan Manager 関数を使用して、より複雑な SQL をベースラインにバインドすることができます。

例えば、バインドする SQL は次のとおりです：

```SQL
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

`i_color in ('slate', 'blanched', 'burnished')` の定数値が同じであるため、SQL は SQL Plan Manager 関数として認識されます：

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

これは、両方の `i_color in ('xxx', 'xxx')` インスタンスが同じパラメータとして認識され、SQL Plan Manager が異なるパラメータを使用する場合にそれらを区別できないことを意味します。このような場合、`BindSQL` と `PlanSQL` でパラメータを手動で指定できます：

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
    where i_color IN (_spm_const_list(2)) and cs_item_sk = i_item_sk
    group by i_item_id
)
select i_item_id, sum(total_sales) total_sales
from (  select * from ss
        union all
        select * from cs) tmp1
group by i_item_id;
```

クエリがベースラインによって書き換えられているかどうかを確認します：

```Plain
mysql> show baseline\G;
***************************[ 1. row ]***************************
Id            | 436115
global        | N
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`store_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`catalog_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(2))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales`
FROM (SELECT *
FROM `ss` UNION ALL SELECT *
FROM `cs`) `tmp1`
GROUP BY `tmp1`.`i_item_id`
.......
***************************[ 2. row ]***************************
Id            | 436119
global        | N
bindSQL       | WITH `ss` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`store_sales`.`ss_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`store_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`store_sales`.`ss_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) , `cs` (`i_item_id`, `total_sales`) AS (SELECT `tpcds`.`item`.`i_item_id`, sum(`tpcds`.`catalog_sales`.`cs_ext_sales_price`) AS `total_sales`
FROM `tpcds`.`catalog_sales` , `tpcds`.`item` 
WHERE (`tpcds`.`item`.`i_color` IN (_spm_const_list(1))) AND (`tpcds`.`catalog_sales`.`cs_item_sk` = `tpcds`.`item`.`i_item_sk`)
GROUP BY `tpcds`.`item`.`i_item_id`) SELECT `tmp1`.`i_item_id`, sum(`tmp1`.`total_sales`) AS `total_sales`
FROM (SELECT *
FROM `ss` UNION ALL SELECT *
FROM `cs`) `tmp1`
GROUP BY `tmp1`.`i_item_id`
.......
2 rows in set
Time: 0.011s

mysql> explain with ss as (
    select i_item_id, sum(ss_ext_sales_price) total_sales
    from store_sales, item
    where i_color IN ("a", "b", "c") and ss_item_sk = i_item_sk
    group by i_item_id
),
cs as (
    select i_item_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, item
    where i_color IN ("A", "B", "D") and cs_item_sk = i_item_sk
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
| Using baseline plan[436115]                                                               |
|                                                                                           |
| PLAN FRAGMENT 0                                                                           |
|  OUTPUT EXPRS:104: i_item_id | 106: sum                                                   |
|   PARTITION: UNPARTITIONED                                                                |
|                                                                                           |
|   RESULT SINK                                                                             |
|                                                                                           |
|   24:EXCHANGE                                                                             |
|                                                                                           |
| PLAN FRAGMENT 1                                                                           |
......
```

上記の出力から、クエリが SQL Plan Manager 関数内の区別されたパラメータを持つベースラインによって書き換えられていることがわかります。

## 今後の計画

将来的には、StarRocks は SQL Plan Manager に基づくより高度な機能を提供する予定です。これには以下が含まれます：
- SQL プランの安定性チェックの強化。
- 固定クエリプランの自動最適化。
- より多くの条件付きパラメータバインディング方法のサポート。
