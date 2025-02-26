---
displayed_sidebar: docs
sidebar_position: 20
---

# ビットマップインデックス

このトピックでは、ビットマップインデックスの作成と管理方法、および使用例について説明します。

## はじめに

ビットマップインデックスは、ビットマップ（ビットの配列）を使用する特別なデータベースインデックスです。ビットは常に0または1のいずれかの値を持ちます。ビットマップ内の各ビットは、テーブル内の単一の行に対応しています。各ビットの値は、対応する行の値に依存します。

ビットマップインデックスは、特定の列のクエリパフォーマンスを向上させるのに役立ちます。クエリのフィルタ条件がプレフィックスインデックスと一致する場合、クエリ効率を大幅に向上させ、迅速に結果を返すことができます。ただし、テーブルには1つのプレフィックスインデックスしか持てません。クエリのフィルタ条件がプレフィックスインデックスのプレフィックスを含まない場合、その列にビットマップインデックスを作成してクエリ効率を向上させることができます。

### クエリを高速化するためのビットマップインデックスの設計方法

ビットマップインデックスを選択する際の主な考慮事項は、**列の基数とクエリに対するビットマップインデックスのフィルタリング効果**です。一般的な誤解とは異なり、StarRocksのビットマップインデックスは、**高基数の列に対するクエリや、複数の低基数列の組み合わせに対するクエリにより適しています。さらに、ビットマップインデックスはデータを効果的にフィルタリングし、少なくとも999/1000のデータをフィルタリングすることで、読み込むページデータの量を減らすことができます。**

単一の低基数列に対するクエリでは、ビットマップインデックスのフィルタリング効果は低く、読み取る必要のある行が多すぎて、複数のページに分散してしまいます。

:::info

ビットマップインデックスがクエリに対して持つフィルタリング効果を評価する際には、データロードのコストを考慮する必要があります。StarRocksでは、基礎データはページ（デフォルトサイズは64K）によって編成され、ロードされます。データロードのコストには、ディスクからページをロードする時間、ページを解凍する時間、デコードする時間が含まれます。

:::

ただし、過度に高い基数は、**より多くのディスクスペースを占有する**などの問題を引き起こす可能性があります。また、ビットマップインデックスはデータロード中に構築される必要があるため、頻繁なデータロードは**ロードパフォーマンスに影響を与える**可能性があります。

さらに、**クエリ中のビットマップインデックスのロードオーバーヘッド**も考慮する必要があります。クエリ中にビットマップインデックスはオンデマンドでロードされ、`クエリ条件に関与する列値の数/基数 x ビットマップインデックス`の値が大きいほど、クエリ中のビットマップインデックスのロードオーバーヘッドが大きくなります。

ビットマップインデックスに適した基数とクエリ条件を決定するには、このトピックの[ビットマップインデックスのパフォーマンステスト](#performance-test-on-bitmap-index)を参照して、パフォーマンステストを実施することをお勧めします。実際のビジネスデータとクエリを使用して、**異なる基数の列にビットマップインデックスを作成し、クエリに対するビットマップインデックスのフィルタリング効果（少なくとも999/1000のデータをフィルタリング）、ディスクスペースの使用量、ロードパフォーマンスへの影響、クエリ中のビットマップインデックスのロードオーバーヘッドを分析することができます。**

StarRocksには、ビットマップインデックスの[適応選択メカニズム](#adaptive-selection-of-bitmap-indexes)が組み込まれています。ビットマップインデックスがクエリを高速化できない場合、たとえば、多くのページをフィルタリングできない場合や、クエリ中のビットマップインデックスのロードオーバーヘッドが高い場合、クエリ中に使用されないため、クエリパフォーマンスに大きな影響を与えることはありません。

### ビットマップインデックスの適応選択

StarRocksは、列の基数とクエリ条件に基づいてビットマップインデックスを使用するかどうかを適応的に選択できます。ビットマップインデックスが多くのページを効果的にフィルタリングできない場合や、クエリ中のビットマップインデックスのロードオーバーヘッドが高い場合、StarRocksはデフォルトでビットマップインデックスを使用しないようにして、クエリパフォーマンスの低下を避けます。

StarRocksは、クエリ条件に関与する値の数と列の基数の比率に基づいてビットマップインデックスを使用するかどうかを判断します。一般に、この比率が小さいほど、ビットマップインデックスのフィルタリング効果が良好です。したがって、StarRocksは`bitmap_max_filter_ratio/1000`をしきい値として使用します。`フィルタ条件の値の数/列の基数`が`bitmap_max_filter_ratio/1000`未満の場合、ビットマップインデックスが使用されます。`bitmap_max_filter_ratio`のデフォルト値は`1`です。

単一の列に基づくクエリの例として、`SELECT * FROM employees WHERE gender = 'male';`があります。`employees`テーブルの`gender`列には、'male'と'female'の値があるため、基数は2（2つの異なる値）です。クエリ条件には1つの値が含まれるため、比率は1/2であり、1/1000より大きいです。したがって、このクエリではビットマップインデックスは使用されません。

複数の列の組み合わせに基づくクエリの例として、`SELECT * FROM employees WHERE gender = 'male' AND city IN ('Beijing', 'Shanghai');`があります。`city`列の基数は10,000であり、クエリ条件には2つの値が含まれるため、比率は`(1*2)/(2*10000)`であり、1/1000より小さいです。したがって、このクエリではビットマップインデックスが使用されます。

:::info

`bitmap_max_filter_ratio`の値の範囲は1-1000です。`bitmap_max_filter_ratio`が`1000`に設定されている場合、ビットマップインデックスを持つ列に対するクエリは強制的にビットマップインデックスを使用します。

:::

### 利点

- ビットマップインデックスは、クエリされた列値の行番号を迅速に特定でき、ポイントクエリや小範囲のクエリに適しています。
- ビットマップインデックスは、和集合や積集合操作（ORおよびAND操作）を含む多次元クエリを最適化できます。

## 考慮事項

### 最適化可能なクエリ

ビットマップインデックスは、等価`=`クエリ、`[NOT] IN`範囲クエリ、`>`, `>=`, `<`, `<=`クエリ、および`IS NULL`クエリの最適化に適しています。`!=`および`[NOT] LIKE`クエリの最適化には適していません。

### サポートされている列とデータ型

ビットマップインデックスは、プライマリキーテーブルおよび重複キーテーブルのすべての列、および集計テーブルとユニークキーテーブルのキー列に作成できます。ビットマップインデックスは、以下のデータ型の列に作成できます。

- 日付型: DATE, DATETIME.
- 数値型: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DECIMAL, BOOLEAN.
- 文字列型: CHAR, STRING, VARCHAR.
- その他の型: HLL.

## 基本操作

### インデックスの作成

- テーブル作成時にビットマップインデックスを作成します。

    ```SQL
    CREATE TABLE `lineorder_partial` (
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_orderdate` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
      `lo_quantity` int(11) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
       INDEX lo_orderdate_index (lo_orderdate) USING BITMAP
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

    この例では、`lo_orderdate`列に`lo_orderdate_index`という名前のビットマップインデックスが作成されています。ビットマップインデックスの命名要件については、[システム制限](../../sql-reference/System_limit.md)を参照してください。同一のビットマップインデックスは同じテーブル内に作成できません。

    複数のビットマップインデックスを複数の列に対して作成することができ、カンマ（,）で区切ります。

    :::note

    テーブル作成の詳細なパラメータについては、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)を参照してください。

    :::

- `CREATE INDEX`を使用して、テーブル作成後にビットマップインデックスを作成できます。詳細なパラメータの説明と例については、[CREATE INDEX](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md)を参照してください。

    ```SQL
    CREATE INDEX lo_quantity_index ON lineorder_partial (lo_quantity) USING BITMAP;
    ```

### インデックス作成の進行状況

ビットマップインデックスの作成は非同期プロセスです。インデックス作成文を実行した後、[SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md)コマンドを使用してインデックス作成の進行状況を確認できます。返された値の`State`フィールドが`FINISHED`を示している場合、インデックスは正常に作成されています。

```SQL
SHOW ALTER TABLE COLUMN;
```

:::info

各テーブルには、同時に進行中のスキーマ変更タスクを1つしか持てません。現在のビットマップインデックスが作成されるまで、新しいビットマップインデックスを作成することはできません。

:::

### インデックスの表示

指定されたテーブルのすべてのビットマップインデックスを表示します。詳細なパラメータと返される結果については、[SHOW INDEX](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_INDEX.md)を参照してください。

```SQL
SHOW INDEXES FROM lineorder_partial;
```

:::note

ビットマップインデックスの作成は非同期プロセスです。上記のステートメントを使用して、正常に作成が完了したインデックスのみを表示できます。

:::

### インデックスの削除

指定されたテーブルのビットマップインデックスを削除します。詳細なパラメータと例については、[DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/DROP_INDEX.md)を参照してください。

```SQL
DROP INDEX lo_orderdate_index ON lineorder_partial;
```

### ビットマップインデックスがクエリを高速化するかどうかを確認する

クエリプロファイルの`BitmapIndexFilterRows`フィールドを確認します。プロファイルの表示方法については、[クエリ分析](../../administration/Query_planning.md)を参照してください。

## ビットマップインデックスのパフォーマンステスト

### テストの目的

異なる基数のクエリに対するビットマップインデックスのフィルタリング効果やディスク使用量などの影響を分析すること：

- [低基数の単一列に対するクエリ](#query-1-query-on-single-column-of-low-cardinality)
- [低基数の複数列の組み合わせに対するクエリ](#query-2-query-on-the-combination-of-multiple-columns-of-low-cardinality)
- [高基数の単一列に基づくクエリ](#query-3-query-based-on-single-high-cardinality-column)

このセクションでは、ビットマップインデックスを常に使用する場合と、ビットマップインデックスを適応的に使用する場合のパフォーマンスを比較し、[StarRocksのビットマップインデックスの適応選択](#adaptive-selection-of-bitmap-indexes)の有効性を検証します。

### テーブルとビットマップインデックスの作成

:::warning

ページデータのキャッシュがクエリパフォーマンスに影響を与えないようにするために、BE構成項目`disable_storage`が`true`に設定されていることを確認してください。

:::

このセクションでは、テーブル`lineorder`（SSB 20G）を例に取ります。

- 参照用のビットマップインデックスなしの元のテーブル

    ```SQL
    CREATE TABLE `lineorder_without_index` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_linenumber` int(11) NOT NULL COMMENT "",
    `lo_custkey` int(11) NOT NULL COMMENT "",
    `lo_partkey` int(11) NOT NULL COMMENT "",
    `lo_suppkey` int(11) NOT NULL COMMENT "",
    `lo_orderdate` int(11) NOT NULL COMMENT "",
    `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
    `lo_shippriority` int(11) NOT NULL COMMENT "",
    `lo_quantity` int(11) NOT NULL COMMENT "",
    `lo_extendedprice` int(11) NOT NULL COMMENT "",
    `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
    `lo_discount` int(11) NOT NULL COMMENT "",
    `lo_revenue` int(11) NOT NULL COMMENT "",
    `lo_supplycost` int(11) NOT NULL COMMENT "",
    `lo_tax` int(11) NOT NULL COMMENT "",
    `lo_commitdate` int(11) NOT NULL COMMENT "",
    `lo_shipmode` varchar(11) NOT NULL COMMENT ""
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

- ビットマップインデックスを持つテーブル: `lo_shipmode`, `lo_quantity`, `lo_discount`, `lo_orderdate`, `lo_tax`, `lo_partkey`に基づいてビットマップインデックスが作成されています。

    ```SQL
    CREATE TABLE `lineorder_with_index` (
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_linenumber` int(11) NOT NULL COMMENT "",
      `lo_custkey` int(11) NOT NULL COMMENT "",
      `lo_partkey` int(11) NOT NULL COMMENT "",
      `lo_suppkey` int(11) NOT NULL COMMENT "",
      `lo_orderdate` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
      `lo_shippriority` int(11) NOT NULL COMMENT "",
      `lo_quantity` int(11) NOT NULL COMMENT "",
      `lo_extendedprice` int(11) NOT NULL COMMENT "",
      `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
      `lo_discount` int(11) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
      `lo_supplycost` int(11) NOT NULL COMMENT "",
      `lo_tax` int(11) NOT NULL COMMENT "",
      `lo_commitdate` int(11) NOT NULL COMMENT "",
      `lo_shipmode` varchar(11) NOT NULL COMMENT "",
      INDEX i_shipmode (`lo_shipmode`) USING BITMAP,
      INDEX i_quantity (`lo_quantity`) USING BITMAP,
      INDEX i_discount (`lo_discount`) USING BITMAP,
      INDEX i_orderdate (`lo_orderdate`) USING BITMAP,
      INDEX i_tax (`lo_tax`) USING BITMAP,
      INDEX i_partkey (`lo_partkey`) USING BITMAP
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

### ビットマップインデックスのディスクスペース使用量

- `lo_shipmode`: 文字列型、基数7、130Mを占有
- `lo_quantity`: 整数型、基数50、291Mを占有
- `lo_discount`: 整数型、基数11、198Mを占有
- `lo_orderdate`: 整数型、基数2406、191Mを占有
- `lo_tax`: 整数型、基数9、160Mを占有
- `lo_partkey`: 整数型、基数600,000、601Mを占有

### クエリ1: 低基数の単一列に対するクエリ

#### ビットマップインデックスなしのテーブルに対するクエリ

**クエリ**:

```SQL
SELECT count(1) FROM lineorder_without_index WHERE lo_shipmode="MAIL";
```

**クエリパフォーマンス分析**: クエリされたテーブルにはビットマップインデックスがないため、`lo_shipmode`列データを含むすべてのページを読み取り、その後に述語フィルタリングが適用されます。

合計時間: 約0.91秒、**データロードに0.47秒**、低基数最適化のための辞書デコードに0.31秒、述語フィルタリングに0.23秒かかります。

```Bash
PullRowNum: 20.566M (20566493) // 結果セットの行数。
CompressedBytesRead: 55.283 MB // 読み取られたデータの総量。
RawRowsRead: 143.999M (143999468) // 読み取られた行数。ビットマップインデックスがないため、この列のすべてのデータが読み取られます。
ReadPagesNum: 8.795K (8795) // 読み取られたページ数。ビットマップインデックスがないため、この列のデータを含むすべてのページが読み取られます。
IOTaskExecTime: 914ms // データスキャンの合計時間。
    BlockFetch: 469ms // データロードの時間。
    DictDecode: 311.612ms // 低基数最適化のための辞書デコードの時間。
    PredFilter: 23.182ms // 述語フィルタリングの時間。
    PredFilterRows: 123.433M (123432975) // フィルタリングされた行数。
```

#### ビットマップインデックスを持つテーブルに対するクエリ

##### ビットマップインデックスを強制的に使用

:::info

StarRocksの設定に従ってビットマップインデックスを強制的に使用するには、各BEノードの**be.conf**ファイルで`bitmap_max_filter_ratio=1000`を設定し、その後BEノードを再起動する必要があります。

:::

**クエリ**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL";
```

**クエリパフォーマンス分析**: クエリされた列は低基数であるため、ビットマップインデックスはデータを効率的にフィルタリングしません。ビットマップインデックスは実際のデータの行番号を迅速に特定できますが、多くの行を読み取る必要があり、複数のページに分散しています。その結果、読み取る必要のあるページを効果的にフィルタリングできません。さらに、ビットマップインデックスのロードとデータフィルタリングのための追加のオーバーヘッドが発生し、合計時間が長くなります。

合計時間: 2.077秒、**データとビットマップインデックスのロードに0.93秒**、低基数最適化のための辞書デコードに0.33秒、ビットマップインデックスによるデータフィルタリングに0.42秒、ZoneMapインデックスによるデータフィルタリングに0.17秒かかります。

```Bash
PullRowNum: 20.566M (20566493) // 結果セットの行数。
CompressedBytesRead: 72.472 MB // 読み取られたデータの総量。この列のビットマップインデックスのサイズは130MBで、7つのユニークな値があります。各値のビットマップインデックスのサイズは18MBです。ページのデータサイズは55MB = 73MBです。
RawRowsRead: 20.566M (20566493) // 読み取られた行数。実際に読み取られた行数は2000万行です。
ReadPagesNum: 8.802K (8802) // 読み取られたページ数。ビットマップインデックスによってフィルタリングされた2000万行が異なるページにランダムに分散しているため、すべてのページが読み取られます。ページは最小のデータ読み取り単位です。したがって、ビットマップインデックスはページをフィルタリングしません。
IOTaskExecTime: 2s77ms // データスキャンの合計時間。ビットマップインデックスなしよりも長いです。
    BlockFetch: 931.144ms // データとビットマップインデックスのロード時間。前のクエリと比較して、ビットマップインデックス（18MB）のロードに追加で400msかかりました。
    DictDecode: 329.696ms // 出力行数が同じであるため、低基数最適化のための辞書デコード時間は同様です。
    BitmapIndexFilter: 419.308ms // ビットマップインデックスによるデータフィルタリングの時間。
    BitmapIndexFilterRows: 123.433M (123432975) // ビットマップインデックスによってフィルタリングされた行数。
    ZoneMapIndexFiter: 171.580ms // ZoneMapインデックスによるデータフィルタリングの時間。
```

##### StarRocksのデフォルト設定に基づいてビットマップインデックスを使用するかどうかを決定

**クエリ**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL";
```

**クエリパフォーマンス分析**: StarRocksのデフォルト設定に従って、フィルタ条件列のユニークな値の数/列の基数 < `bitmap_max_filter_ratio/1000`（デフォルトは1/1000）の場合にビットマップインデックスが使用されます。この場合、値は1/1000より大きいため、ビットマップインデックスは使用されず、ビットマップインデックスなしのテーブルをクエリする場合と同様のパフォーマンスになります。

```Bash
PullRowNum: 20.566M (20566493) // 結果セットの行数。
CompressedBytesRead: 55.283 MB // 読み取られたデータの総量。
RawRowsRead: 143.999M (143999468) // 読み取られた行数。ビットマップインデックスが使用されていないため、この列のすべてのデータが読み取られます。
ReadPagesNum: 8.800K (8800) // 読み取られたページ数。ビットマップインデックスが使用されていないため、この列のデータを含むすべてのページが読み取られます。
IOTaskExecTime: 914.279ms // データスキャンの合計時間。
    BlockFetch: 475.890ms // データロードの時間。
    DictDecode: 312.019ms // 低基数最適化のための辞書デコードの時間。
    PredFilterRows: 123.433M (123432975) // 述語によってフィルタリングされた行数。
```

### クエリ2: 低基数の複数列の組み合わせに対するクエリ

#### ビットマップインデックスなしのテーブルに対するクエリ

**クエリ**:

```SQL
SELECT count(1) 
FROM lineorder_without_index 
WHERE lo_shipmode = "MAIL" 
  AND lo_quantity = 10 
  AND lo_discount = 9 
  AND lo_tax = 8;
```

**クエリパフォーマンス分析**: クエリされたテーブルにはビットマップインデックスがないため、`lo_shipmode`、`lo_quantity`、`lo_discount`、`lo_tax`列のデータを含むすべてのページを読み取り、その後に述語フィルタリングが適用されます。

合計時間: 1.76秒、**データロードに1.6秒（4列）**、述語フィルタリングに0.1秒かかります。

```Bash
PullRowNum: 4.092K (4092) // 結果セットの行数。
CompressedBytesRead: 305.346 MB // 読み取られたデータの総量。4列のデータが読み取られ、合計305MBです。
RawRowsRead: 143.999M (143999468) // 読み取られた行数。ビットマップインデックスがないため、これらの4列のすべてのデータが読み取られます。
ReadPagesNum: 35.168K (35168) // 読み取られたページ数。ビットマップインデックスがないため、これらの4列のデータを含むすべてのページが読み取られます。
IOTaskExecTime: 1s761ms // データスキャンの合計時間。
    BlockFetch: 1s639ms // データロードの時間。
    PredFilter: 96.533ms // 4つのフィルタ条件を持つ述語フィルタリングの時間。
    PredFilterRows: 143.995M (143995376) // 述語によってフィルタリングされた行数。
```

#### ビットマップインデックスを持つテーブルに対するクエリ

##### ビットマップインデックスを強制的に使用

:::info

StarRocksの設定に従ってビットマップインデックスを強制的に使用するには、各BEノードの**be.conf**ファイルで`bitmap_max_filter_ratio=1000`を設定し、その後BEノードを再起動する必要があります。

:::

**クエリ**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL" AND lo_quantity=10 AND lo_discount=9 AND lo_tax=8;
```

**クエリパフォーマンス分析**: これは複数の低基数列に基づく組み合わせクエリであるため、ビットマップインデックスは効果的であり、ページの一部をフィルタリングし、データ読み取り時間を大幅に短縮します。

合計時間: 0.68秒、**データとビットマップインデックスのロードに0.54秒**、ビットマップインデックスによるデータフィルタリングに0.14秒かかります。

```Bash
PullRowNum: 4.092K (4092) // 結果セットの行数。
CompressedBytesRead: 156.340 MB // 読み取られたデータの総量。ビットマップインデックスはデータの2/3を効果的にフィルタリングします。この156MBのうち、60MBはインデックスデータで、90MBは実際のデータです。
ReadPagesNum: 11.325K (11325) // 読み取られたページ数。ビットマップインデックスはページの2/3を効果的にフィルタリングします。
IOTaskExecTime: 683.471ms // データスキャンの合計時間。ビットマップインデックスなしよりも大幅に短いです。
    BlockFetch: 537.421ms // データとビットマップインデックスのロード時間。ビットマップインデックスのロードに追加の時間がかかりますが、データロード時間を大幅に短縮します。
    BitmapIndexFilter: 139.024ms // ビットマップインデックスによるデータフィルタリングの時間。
    BitmapIndexFilterRows: 143.995M (143995376) // ビットマップインデックスによってフィルタリングされた行数。
```

##### StarRocksのデフォルト設定に基づいてビットマップインデックスを使用するかどうかを決定

**クエリ**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL" AND lo_quantity=10 AND lo_discount=9 AND lo_tax=8;
```

**クエリパフォーマンス分析**: StarRocksのデフォルト設定に従って、フィルタ条件列のユニークな値の数/列の基数 < `bitmap_max_filter_ratio/1000`（デフォルトは1/1000）の場合にビットマップインデックスが使用されます。この場合、値は1/1000より小さいため、ビットマップインデックスが使用され、ビットマップインデックスを強制的に使用する場合と同様のパフォーマンスになります。

合計時間: 0.67秒、**データとビットマップインデックスのロードに0.54秒**、ビットマップインデックスによるデータフィルタリングに0.13秒かかります。

```Bash
PullRowNum: 4.092K (4092) // 結果セットの行数。
CompressedBytesRead: 154.430 MB // 読み取られたデータの総量。
ReadPagesNum: 11.209K (11209) // 読み取られたページ数。
IOTaskExecTime: 672.029ms // データスキャンの合計時間。ビットマップインデックスなしよりも大幅に短いです。
    BlockFetch: 535.823ms // データとビットマップインデックスのロード時間。ビットマップインデックスのロードに少し時間がかかりますが、データロード時間を大幅に短縮します。
    BitmapIndexFilter: 128.403ms // ビットマップインデックスによるデータフィルタリングの時間。
    BitmapIndexFilterRows: 143.995M (143995376) // ビットマップインデックスによってフィルタリングされた行数。
```

### クエリ3: 高基数の単一列に対するクエリ

#### ビットマップインデックスなしのテーブルに対するクエリ

**クエリ**:

```SQL
select count(1) from lineorder_without_index where lo_partkey=10000;
```

**クエリパフォーマンス分析**: クエリされたテーブルにはビットマップインデックスがないため、`lo_partkey`列データを含むページを読み取り、その後に述語フィルタリングが適用されます。

合計時間: 約0.43秒、**データロードに0.39秒**、述語フィルタリングに0.02秒かかります。

```Bash
PullRowNum: 255 // 結果セットの行数。
CompressedBytesRead: 344.532 MB // 読み取られたデータの総量。
RawRowsRead: 143.999M (143,999,468) // 読み取られた行数。ビットマップインデックスがないため、この列のすべてのデータが読み取られます。
ReadPagesNum: 8.791K (8,791) // 読み取られたページ数。ビットマップインデックスがないため、この列のデータを含むすべてのページが読み取られます。
IOTaskExecTime: 428.258ms // データスキャンの合計時間。
    BlockFetch: 392.434ms // データロードの時間。
    PredFilter: 20.807ms // 述語フィルタリングの時間。
    PredFilterRows: 143.999M (143,999,213) // 述語によってフィルタリングされた行数。
```

#### ビットマップインデックスを持つテーブルに対するクエリ

##### ビットマップインデックスを強制的に使用

:::info

StarRocksの設定に従ってビットマップインデックスを強制的に使用するには、各BEノードの**be.conf**ファイルで`bitmap_max_filter_ratio=1000`を設定し、その後BEノードを再起動する必要があります。

:::

**クエリ**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_partkey=10000;
```

**クエリパフォーマンス分析**: クエリされた列は高基数であるため、ビットマップインデックスは効果的であり、ページの一部をフィルタリングし、データ読み取り時間を大幅に短縮します。

合計時間: 0.015秒、**データとビットマップインデックスのロードに0.009秒**、ビットマップインデックスによるフィルタリングに0.003秒かかります。

```Bash
PullRowNum: 255 // 結果セットの行数。
CompressedBytesRead: 13.600 MB // 読み取られたデータの総量。ビットマップインデックスのフィルタリングが効果的で、大量のデータをフィルタリングします。
RawRowsRead: 255 // 読み取られた行数。
ReadPagesNum: 225 // 読み取られたページ数。ビットマップインデックスはデータを効果的にフィルタリングし、多くのページをフィルタリングします。
IOTaskExecTime: 15.354ms // データスキャンの合計時間。ビットマップインデックスなしの合計時間よりも大幅に短いです。
    BlockFetch: 9.530ms // データとビットマップインデックスのロード時間。ビットマップインデックスのロードに追加の時間がかかりますが、データロード時間を大幅に短縮します。
    BitmapIndexFilter: 3.450ms // ビットマップインデックスによるデータフィルタリングの時間。
    BitmapIndexFilterRows: 143.999M (143,999,213) // ビットマップインデックスによってフィルタリングされた行数。
```

##### StarRocksのデフォルト設定に基づいてビットマップインデックスを使用するかどうかを決定

**クエリ**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_partkey=10000;
```

**クエリパフォーマンス分析**: StarRocksのデフォルト設定に従って、ビットマップインデックスは、ユニークな値の数/列の基数 < `bitmap_max_filter_ratio/1000`（デフォルト1/1000）の場合に使用されます。この条件が満たされているため、クエリはビットマップインデックスを使用し、ビットマップインデックスを強制的に使用する場合と同様のパフォーマンスを示します。

合計時間: 0.014秒、**データとビットマップインデックスのロードに0.008秒**、ビットマップインデックスによるフィルタリングに0.003秒かかります。

```Bash
PullRowNum: 255 // 結果セットの行数。
CompressedBytesRead: 13.600 MB // 読み取られたデータの総量。ビットマップインデックスはデータを効果的にフィルタリングし、多くのデータをフィルタリングします。
RawRowsRead: 255 // 読み取られた行数。
ReadPagesNum: 225 // 読み取られたページ数。ビットマップインデックスはデータを効果的にフィルタリングし、多くのページをフィルタリングします。
IOTaskExecTime: 14.387ms // データスキャンの合計時間。ビットマップインデックスなしの合計時間よりも大幅に短いです。
    BitmapIndexFilter: 2.979ms // ビットマップインデックスによるデータフィルタリングの時間。
    BitmapIndexFilterRows: 143.999M (143,999,213) // ビットマップインデックスによってフィルタリングされた行数。
    BlockFetch: 8.988ms // データとビットマップインデックスのロード時間。ビットマップインデックスのロードに追加の時間がかかりますが、データロード時間を大幅に短縮します。
```
