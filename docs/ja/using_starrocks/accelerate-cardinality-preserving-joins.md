---
displayed_sidebar: docs
sidebar_position: 120
---

# 基数保持ジョインの高速化

このトピックでは、テーブルプルーニングを使用して基数保持ジョインを高速化する方法について説明します。この機能は v3.1 以降でサポートされています。

## 概要

基数保持ジョインは、出力行の基数と重複係数がジョインの入力テーブルの1つと同じままであることを保証します。以下の例を考えてみましょう。

- **Inner Join:**

  ```SQL
  SELECT A.* FROM A INNER JOIN B ON A.fk = B.pk;
  ```

  この場合、`A.fk`（外部キー）は **NOT NULL** であり、`B.pk`（主キー）を参照しています。`A` の各行は `B` のちょうど1行と一致するため、出力の基数と重複係数は `A` のものと一致します。

- **Left Join:**

  ```SQL
  SELECT A.* FROM A LEFT JOIN B ON A.fk = B.pk;
  ```

  ここでは、`A.fk` は `B.pk` を参照していますが、`A.fk` は NULL 値を含むことがあります。`A` の各行は `B` の最大1行と一致します。その結果、出力の基数と重複係数は `A` と一致したままです。

<br />

これらのタイプのジョインでは、最終的な出力列がテーブル `A` の列にのみ依存し、テーブル `B` の列が使用されない場合、テーブル `B` はジョインからプルーニングできます。v3.1 以降、StarRocks は基数保持ジョインでの **テーブルプルーニング** をサポートしており、これは共通テーブル式 (CTE)、ビュー、およびサブクエリで発生する可能性があります。

### ユースケース: リスク管理におけるリアルタイム特徴選択

基数保持ジョインのためのテーブルプルーニング機能は、リスク管理のための **リアルタイム特徴選択** のようなシナリオで特に有用です。このコンテキストでは、ユーザーは多くのテーブルからデータを選択する必要があり、しばしば列とテーブルの組み合わせ爆発に対処します。リスク管理ドメインでは、以下の特性が一般的です。

- 多数の特徴が独立して更新される多くのテーブルに分散している。
- 新しいデータはリアルタイムで可視化およびクエリ可能でなければならない。
- データモデルを簡素化するために **フラットなビュー** が使用され、列抽出のための SQL がより簡潔で生産的になる。

フラットなビューを使用することで、他の加速データレイヤーよりも効率的にリアルタイムデータにアクセスできます。各列抽出クエリでは、ビュー内のすべてのテーブルではなく、いくつかのテーブルのみがジョインされる必要があります。これらのクエリから未使用のテーブルをプルーニングすることで、ジョインの数を減らし、パフォーマンスを向上させることができます。

### 機能サポート

テーブルプルーニング機能は、**スタースキーマ** および **スノーフレークスキーマ** の両方でのマルチテーブルジョインをサポートしています。マルチテーブルジョインは、CTE、ビュー、およびサブクエリに現れることができ、より効率的なクエリ実行を可能にします。

現在、テーブルプルーニング機能は OLAP テーブルとクラウドネイティブテーブルでのみサポートされています。マルチジョインの外部テーブルはプルーニングできません。

## 使用方法

以下の例では、TPC-H データセットを使用します。

### 前提条件

テーブルプルーニング機能を使用するには、次の条件を満たす必要があります。

1. テーブルプルーニングを有効にする
2. キー制約を設定する

#### テーブルプルーニングを有効にする

デフォルトでは、テーブルプルーニングは無効になっています。この機能を有効にするには、次のセッション変数を設定する必要があります。

```SQL
-- RBOフェーズのテーブルプルーニングを有効にする。
SET enable_rbo_table_prune=true;
-- CBOフェーズのテーブルプルーニングを有効にする。
SET enable_cbo_table_prune=true; 
-- 主キーテーブルのUPDATE文に対するRBOフェーズのテーブルプルーニングを有効にする。
SET enable_table_prune_on_update = true;
```

#### キー制約を設定する

プルーニングされるテーブルは、少なくとも LEFT または RIGHT ジョインでユニークキーまたは主キー制約を持っている必要があります。INNER JOIN でテーブルをプルーニングするには、ユニークキーまたは主キー制約に加えて外部キー制約を定義する必要があります。

主キーテーブルとユニークキーテーブルには、暗黙の主キーまたはユニークキー制約が自然に組み込まれています。しかし、重複キーテーブルの場合、ユニークキー制約を手動で定義し、重複行が存在しないことを確認する必要があります。StarRocks は重複キーテーブルに対してユニークキー制約を強制しません。代わりに、より積極的なクエリ計画のための最適化ヒントとして扱います。

例:

```SQL
-- テーブル作成時にユニークキー制約を定義する。
CREATE TABLE `lineitem` (
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11) NOT NULL COMMENT "",
  `l_linenumber` int(11) NOT NULL COMMENT "",
  `l_quantity` decimal64(15, 2) NOT NULL COMMENT "",
  `l_extendedprice` decimal64(15, 2) NOT NULL COMMENT "",
  `l_discount` decimal64(15, 2) NOT NULL COMMENT "",
  `l_tax` decimal64(15, 2) NOT NULL COMMENT "",
  `l_returnflag` varchar(1) NOT NULL COMMENT "",
  `l_linestatus` varchar(1) NOT NULL COMMENT "",
  `l_shipdate` date NOT NULL COMMENT "",
  `l_commitdate` date NOT NULL COMMENT "",
  `l_receiptdate` date NOT NULL COMMENT "",
  `l_shipinstruct` varchar(25) NOT NULL COMMENT "",
  `l_shipmode` varchar(10) NOT NULL COMMENT "",
  `l_comment` varchar(44) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`l_orderkey`,`l_partkey`, `l_suppkey`)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96 
PROPERTIES (
"unique_constraints" = "l_orderkey,l_linenumber"
); 

-- または、テーブル作成後にユニークキー制約を定義することができます。
ALTER TABLE lineitem SET ("unique_constraints" = "l_orderkey,l_linenumber");
```

一方、外部キー制約は明示的に定義する必要があります。重複キーテーブルのユニークキー制約と同様に、外部キー制約はオプティマイザのためのヒントとして機能します。StarRocks は外部キー制約の整合性を強制しません。StarRocks にデータを生成および取り込む際にデータの整合性を確保する必要があります。

例:

```SQL
-- 外部キー制約で参照されるテーブルを作成する。
-- 参照される列にはユニークキーまたは主キー制約が必要です。
-- この例では、`p_partkey` はテーブル `part` の主キーです。
CREATE TABLE part (
    p_partkey     int(11) NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INT NOT NULL,
    p_container   CHAR(10) NOT NULL,
    p_retailprice DOUBLE NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
) ENGINE=OLAP
PRIMARY KEY(`p_partkey`)
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12;

-- テーブル作成時に外部キー制約を定義する。
CREATE TABLE `lineitem` (
  `l_orderkey` int(11) NOT NULL COMMENT "",
  `l_partkey` int(11) NOT NULL COMMENT "",
  `l_suppkey` int(11) NOT NULL COMMENT "",
  `l_linenumber` int(11) NOT NULL COMMENT "",
  `l_quantity` decimal64(15, 2) NOT NULL COMMENT "",
  `l_extendedprice` decimal64(15, 2) NOT NULL COMMENT "",
  `l_discount` decimal64(15, 2) NOT NULL COMMENT "",
  `l_tax` decimal64(15, 2) NOT NULL COMMENT "",
  `l_returnflag` varchar(1) NOT NULL COMMENT "",
  `l_linestatus` varchar(1) NOT NULL COMMENT "",
  `l_shipdate` date NOT NULL COMMENT "",
  `l_commitdate` date NOT NULL COMMENT "",
  `l_receiptdate` date NOT NULL COMMENT "",
  `l_shipinstruct` varchar(25) NOT NULL COMMENT "",
  `l_shipmode` varchar(10) NOT NULL COMMENT "",
  `l_comment` varchar(44) NOT NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATEK KEY(`l_orderkey`,`l_partkey`, `l_suppkey`)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96 
PROPERTIES (
"foreign_key_constraints" =  "(l_partkey) REFERENCES part(p_partkey)"
);

-- または、テーブル作成後に外部キー制約を定義することができます。
ALTER TABLE lineitem SET ("foreign_key_constraints" =  "(l_partkey) REFERENCES part(p_partkey)");
```

### ユニークキーまたは主キーに基づく LEFT/RIGHT JOIN でのテーブルプルーニング

LEFT または RIGHT JOIN でのテーブルプルーニングでは、ジョインの保持側がプルーニング側を参照する外部キーを持つ必要はありません。これにより、参照整合性が保証されなくても、プルーニングがより柔軟で堅牢になります。

ユニークキーまたは主キーに基づく LEFT/RIGHT JOIN でのプルーニングは、外部キーに基づく INNER JOIN プルーニングと比較して **要件が緩和されています**。

プルーニングの条件は次のとおりです。

- **プルーニング側**

  プルーニングされるテーブルは、LEFT JOIN では **右側**、RIGHT JOIN では **左側** でなければなりません。

- **ジョイン条件**

  ジョインは等価条件 (`=`) のみを使用し、プルーニング側の結合された列はユニークキーまたは主キーの **スーパーセット** でなければなりません。

- **出力列**

  保持側の列のみが出力され、結果は保持側と **同じ基数と重複係数** を維持する必要があります。

- **NULL/デフォルト値**

  保持側の結合された列は、プルーニング側と一致しない NULL または他のデフォルト値を含むことができます。

例:

1. テーブルを作成し、データを挿入します。

    ```SQL
    -- テーブル `depts` は、列 `deptno` に主キー制約があります。
    CREATE TABLE `depts` (
    `deptno` int(11) NOT NULL COMMENT "",
    `name` varchar(25) NOT NULL COMMENT ""
    ) ENGINE=OLAP
    PRIMARY KEY(`deptno`)
    DISTRIBUTED BY HASH(`deptno`) BUCKETS 10;
    
    CREATE TABLE `emps` (
    `empid` int(11) NOT NULL COMMENT "",
    `deptno` int(11) NOT NULL COMMENT "",
    `name` varchar(25) NOT NULL COMMENT "",
    `salary` double NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`empid`)
    DISTRIBUTED BY HASH(`empid`) BUCKETS 10;
    
    INSERT INTO depts VALUES
    (1, "R&D"),
    (2, "Marketing"), 
    (3, "Community"),
    (4, "DBA"),(5, "POC");
    
    INSERT INTO emps VALUES
    (1, 1, "Alice", "6000"), 
    (2, 1, "Bob", "6100"),
    (3, 2, "Candy", "10000"),
    (4, 2, "Dave", "20000"),
    (5, 3, "Evan","18000"),
    (6, 3, "Freman","1000"),
    (7, 4, "George","1800"),
    (8, 4, "Harry","2000"),
    (9, 5, "Ivan", "15000"),
    (10, 5, "Jim","20000"),
    (11, -1, "Kevin","1500"),
    (12, -1, "Lily","2500");
    ```

2. クエリの論理実行計画を確認します。

    ```SQL
    -- Q1: `emps` のすべての列をクエリします。`depts` のすべての列はクエリされません。
    EXPLAIN LOGICAL SELECT emps.* FROM emps LEFT JOIN depts ON emps.deptno = depts.deptno;
    +-----------------------------------------------------------------------------+
    | Explain String                                                              |
    +-----------------------------------------------------------------------------+
    | - Output => [1:empid, 2:deptno, 3:name, 4:salary]                           |
    |     - SCAN [emps] => [1:empid, 2:deptno, 3:name, 4:salary]                  |
    |             Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 20.0} |
    |             partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-----------------------------------------------------------------------------+
    
    -- Q2: `emps` の `deptno` と `salary` のみをクエリします。`depts` の列はクエリされません。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary 
    FROM emps LEFT JOIN depts ON emps.deptno = depts.deptno 
    GROUP BY emps.deptno 
    ORDER BY mean_salary DESC 
    LIMIT 5;
    +-------------------------------------------------------------------------------------------------+
    | Explain String                                                                                  |
    +-------------------------------------------------------------------------------------------------+
    | - Output => [2:deptno, 7:avg]                                                                   |
    |     - TOP-5(FINAL)[7: avg DESC NULLS LAST]                                                      |
    |             Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 138.0}                     |
    |         - TOP-5(PARTIAL)[7: avg DESC NULLS LAST]                                                |
    |                 Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 114.0}                 |
    |             - AGGREGATE(GLOBAL) [2:deptno]                                                      |
    |                     Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 90.0}              |
    |                     7:avg := avg(7:avg)                                                         |
    |                 - EXCHANGE(SHUFFLE) [2]                                                         |
    |                         Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 72.0}          |
    |                     - AGGREGATE(LOCAL) [2:deptno]                                               |
    |                             Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 48.0}      |
    |                             7:avg := avg(4:salary)                                              |
    |                         - SCAN [emps] => [2:deptno, 4:salary]                                   |
    |                                 Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 12.0} |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-------------------------------------------------------------------------------------------------+
    
    -- Q3: `emps` の列のみがクエリされます。`name = "R&D"` の述語は `depts` の特定の行のみを選択しますが、最終結果は `emps` のみに依存します。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary
    FROM emps LEFT JOIN
    (SELECT deptno FROM depts WHERE name="R&D") t ON emps.deptno = t.deptno 
    GROUP BY emps.deptno 
    ORDER BY mean_salary DESC
    LIMIT 5;
    +-------------------------------------------------------------------------------------------------+
    | Explain String                                                                                  |
    +-------------------------------------------------------------------------------------------------+
    | - Output => [2:deptno, 7:avg]                                                                   |
    |     - TOP-5(FINAL)[7: avg DESC NULLS LAST]                                                      |
    |             Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 138.0}                     |
    |         - TOP-5(PARTIAL)[7: avg DESC NULLS LAST]                                                |
    |                 Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 114.0}                 |
    |             - AGGREGATE(GLOBAL) [2:deptno]                                                      |
    |                     Estimates: {row: 3, cpu: ?, memory: ?, network: ?, cost: 90.0}              |
    |                     7:avg := avg(7:avg)                                                         |
    |                 - EXCHANGE(SHUFFLE) [2]                                                         |
    |                         Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 72.0}          |
    |                     - AGGREGATE(LOCAL) [2:deptno]                                               |
    |                             Estimates: {row: 6, cpu: ?, memory: ?, network: ?, cost: 48.0}      |
    |                             7:avg := avg(4:salary)                                              |
    |                         - SCAN [emps] => [2:deptno, 4:salary]                                   |
    |                                 Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 12.0} |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-------------------------------------------------------------------------------------------------+
    
    -- Q4: WHERE句の述語 `depts.name="R&D"` は基数保持条件を破るため、`depts` はプルーニングできません。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary 
    FROM emps LEFT JOIN depts ON emps.deptno = depts.deptno 
    WHERE depts.name="R&D";
    
    +-----------------------------------------------------------------------------------------------+
    | Explain String                                                                                |
    +-----------------------------------------------------------------------------------------------+
    | - Output => [8:any_value, 7:avg]                                                              |
    |     - AGGREGATE(GLOBAL) []                                                                    |
    |             Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 110.5}                   |
    |             7:avg := avg(7:avg)                                                               |
    |             8:any_value := any_value(8:any_value)                                             |
    |         - EXCHANGE(GATHER)                                                                    |
    |                 Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 105.5}               |
    |             - AGGREGATE(LOCAL) []                                                             |
    |                     Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 101.5}           |
    |                     7:avg := avg(4:salary)                                                    |
    |                     8:any_value := any_value(2:deptno)                                        |
    |                 - HASH/INNER JOIN [2:deptno = 5:deptno] => [2:deptno, 4:salary]               |
    |                         Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 79.5}       |
    |                     - SCAN [emps] => [2:deptno, 4:salary]                                     |
    |                             Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 12.0}   |
    |                             partitionRatio: 1/1, tabletRatio: 10/10                           |
    |                     - EXCHANGE(BROADCAST)                                                     |
    |                             Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 25.0}    |
    |                         - SCAN [depts] => [5:deptno]                                          |
    |                                 Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 5.0} |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                       |
    |                                 predicate: 6:name = 'R&D'                                     |
    +-----------------------------------------------------------------------------------------------+
    ```

上記の例では、実行計画に示されているように、Q1、Q2、およびQ3でテーブルプルーニングが許可されていますが、基数保持条件を破るQ4ではテーブルプルーニングが実行されません。

### 外部キーに基づく INNER/LEFT/RIGHT JOIN でのテーブルプルーニング

**INNER JOIN** でのテーブルプルーニングはより制限が厳しく、保持側がプルーニング側を参照する **外部キー** を持ち、参照整合性が保証されている必要があります。現在、StarRocks は外部キー制約の整合性チェックを強制しません。

外部キーに基づくテーブルプルーニングはより厳しいですが、より強力です。オプティマイザが INNER JOIN で列の等価性推論を活用できるため、より複雑なシナリオでプルーニングが可能になります。

プルーニングの条件は次のとおりです。

- **プルーニング側**

  - LEFT JOIN では、プルーニングされるテーブルは右側にある必要があります。RIGHT JOIN では、左側にある必要があります。
  - INNER JOIN では、プルーニングされるテーブルは結合された列にユニークキー制約を持ち、保持側の各行がプルーニング側のちょうど1行と一致する必要があります。保持側に一致しない行が存在する場合、プルーニングは行えません。

- **ジョイン条件**

  ジョインは等価条件 (`=`) のみを使用する必要があります。保持側の結合された列は **外部キー** であり、プルーニング側の結合された列は **主キー** または **ユニークキー** である必要があります。これらの列は外部キー制約に従って整列する必要があります。

- **出力列**

  - 保持側の列のみが出力され、結果は保持側と **同じ基数と重複係数** を維持する必要があります。
  - INNER JOIN では、プルーニング側の結合された列は保持側の同等の列で置き換えることができます。プルーニング側のすべての出力列が結合された列である場合、プルーニングも可能です。

- **NULL/デフォルト値:**

  保持側の結合された列は、プルーニング側と一致しない NULL または他のデフォルト値を含むことはできません。

例:

1. テーブルを作成し、外部キーを定義し、データを挿入します。

    ```SQL
    CREATE TABLE `depts` (
    `deptno` int(11) NOT NULL COMMENT "",
    `name` varchar(25) NOT NULL COMMENT ""
    ) ENGINE=OLAP
    PRIMARY KEY(`deptno`)
    DISTRIBUTED BY HASH(`deptno`) BUCKETS 10;
    
    CREATE TABLE `emps` (
    `empid` int(11) NOT NULL COMMENT "",
    `deptno` int(11) NOT NULL COMMENT "",
    `name` varchar(25) NOT NULL COMMENT "",
    `salary` double NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`empid`)
    DISTRIBUTED BY HASH(`empid`) BUCKETS 10;
    
    ALTER TABLE emps SET ("foreign_key_constraints" = "(deptno) REFERENCES depts(deptno)");
    
    INSERT INTO depts VALUES
    (1, "R&D"),
    (2, "Marketing"), 
    (3, "Community"),
    (4, "DBA"),(5, "POC");
    
    INSERT INTO emps VALUES
    (1, 1, "Alice", "6000"), 
    (2, 1, "Bob", "6100"),
    (3, 2, "Candy", "10000"),
    (4, 2, "Dave", "20000"),
    (5, 3, "Evan","18000"),
    (6, 3, "Freman","1000"),
    (7, 4, "George","1800"),
    (8, 4, "Harry","2000"),
    (9, 5, "Ivan", "15000"),
    (10, 5, "Jim","20000");
    ```

2. クエリの論理実行計画を確認します。

    ```SQL
    -- Q1: `emps` の `empid` と `name`、`depts` の `deptno` をクエリします。
    -- ただし、ジョイン条件に示されているように、`emps.deptno` は `depts.deptno` と同等であるため、
    -- `emps.deptno` は `depts.deptno` の代わりに使用できます。
    EXPLAIN LOGICAL WITH t0 AS (
    SELECT empid, depts.deptno, emps.name, emps.salary, depts.name AS dept_name 
    FROM emps INNER JOIN depts ON emps.deptno = depts.deptno
    )
    SELECT empid, deptno, name FROM t0;
    +-----------------------------------------------------------------------------+
    | Explain String                                                              |
    +-----------------------------------------------------------------------------+
    | - Output => [7:empid, 8:deptno, 9:name]                                     |
    |     - SCAN [emps] => [7:empid, 8:deptno, 9:name]                            |
    |             Estimates: {row: 12, cpu: ?, memory: ?, network: ?, cost: 18.0} |
    |             partitionRatio: 1/1, tabletRatio: 10/10                         |
    +-----------------------------------------------------------------------------+
    
    -- Q2: `emps` の `salary` のみをクエリします。
    EXPLAIN LOGICAL SELECT avg(salary) 
    FROM emps INNER JOIN depts ON emps.deptno = depts.deptno;
    +----------------------------------------------------------------------------------------+
    | Explain String                                                                         |
    +----------------------------------------------------------------------------------------+
    | - Output => [7:avg]                                                                    |
    |     - AGGREGATE(GLOBAL) []                                                             |
    |             Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 16.5}             |
    |             7:avg := avg(7:avg)                                                        |
    |         - EXCHANGE(GATHER)                                                             |
    |                 Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 14.0}         |
    |             - AGGREGATE(LOCAL) []                                                      |
    |                     Estimates: {row: 1, cpu: ?, memory: ?, network: ?, cost: 12.0}     |
    |                     7:avg := avg(4:salary)                                             |
    |                 - SCAN [emps] => [4:salary]                                            |
    |                         Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 5.0} |
    |                         partitionRatio: 1/1, tabletRatio: 10/10                        |
    +----------------------------------------------------------------------------------------+
    
    -- Q3: 述語 `name="R&D"` は最終結果の基数に影響を与えます。
    EXPLAIN LOGICAL SELECT emps.deptno, avg(salary) AS mean_salary 
    FROM emps INNER JOIN
    (SELECT deptno FROM depts WHERE name="R&D") t ON emps.deptno = t.deptno 
    GROUP BY emps.deptno
    ORDER BY mean_salary DESC 
    LIMIT 5;
    +-------------------------------------------------------------------------------------------------------+
    | Explain String                                                                                        |
    +-------------------------------------------------------------------------------------------------------+
    | - Output => [2:deptno, 7:avg]                                                                         |
    |     - TOP-5(FINAL)[7: avg DESC NULLS LAST]                                                            |
    |             Estimates: {row: 2, cpu: ?, memory: ?, network: ?, cost: 165.0480769230769}               |
    |         - TOP-5(PARTIAL)[7: avg DESC NULLS LAST]                                                      |
    |                 Estimates: {row: 2, cpu: ?, memory: ?, network: ?, cost: 145.0480769230769}           |
    |             - AGGREGATE(GLOBAL) [2:deptno]                                                            |
    |                     Estimates: {row: 2, cpu: ?, memory: ?, network: ?, cost: 125.04807692307692}      |
    |                     7:avg := avg(4:salary)                                                            |
    |                 - HASH/INNER JOIN [2:deptno = 5:deptno] => [2:deptno, 4:salary]                       |
    |                         Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 100.04807692307692} |
    |                     - EXCHANGE(SHUFFLE) [2]                                                           |
    |                             Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 50.0}           |
    |                         - SCAN [emps] => [2:deptno, 4:salary]                                         |
    |                                 Estimates: {row: 10, cpu: ?, memory: ?, network: ?, cost: 10.0}       |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                               |
    |                     - EXCHANGE(SHUFFLE) [5]                                                           |
    |                             Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 15.0}            |
    |                         - SCAN [depts] => [5:deptno]                                                  |
    |                                 Estimates: {row: 5, cpu: ?, memory: ?, network: ?, cost: 5.0}         |
    |                                 partitionRatio: 1/1, tabletRatio: 10/10                               |
    |                                 predicate: 6:name = 'R&D'                                             |
    +-------------------------------------------------------------------------------------------------------+
    ```

上記の例では、実行計画に示されているように、Q1 および Q2 でテーブルプルーニングが許可されていますが、述語が最終結果の基数に影響を与える Q3 ではテーブルプルーニングが実行されません。