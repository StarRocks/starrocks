---
displayed_sidebar: docs
toc_max_heading_level: 4
sidebar_position: 50
---

# [プレビュー] 全文インバーテッドインデックス

バージョン 3.3.0 以降、StarRocks は全文インバーテッドインデックスをサポートしています。これにより、テキストを小さな単語に分割し、各単語に対してインデックスエントリを作成できます。このエントリは、単語とデータファイル内の対応する行番号とのマッピング関係を示します。全文検索では、StarRocks は検索キーワードに基づいてインバーテッドインデックスをクエリし、キーワードに一致するデータ行を迅速に特定します。

全文インバーテッドインデックスは、プライマリキーテーブルと共有データクラスタではまだサポートされていません。

## 概要

StarRocks は、基盤となるデータを列ごとに整理されたデータファイルに保存します。各データファイルには、インデックス化された列に基づく全文インバーテッドインデックスが含まれています。インデックス化された列の値は、個々の単語にトークン化されます。トークン化後の各単語はインデックスエントリとして扱われ、その単語が出現する行番号にマッピングされます。現在サポートされているトークン化方法は、英語トークン化、中国語トークン化、多言語トークン化、およびトークン化なしです。

例えば、データ行に「hello world」が含まれ、その行番号が 123 の場合、全文インバーテッドインデックスはこのトークン化結果と行番号に基づいてインデックスエントリを構築します: hello->123, world->123。

全文検索中、StarRocks は全文インバーテッドインデックスを使用して検索キーワードを含むインデックスエントリを特定し、キーワードが出現する行番号を迅速に見つけることができ、スキャンする必要のあるデータ行の数を大幅に削減します。

## 基本操作

### 全文インバーテッドインデックスの作成

全文インバーテッドインデックスを作成する前に、FE 設定項目 `enable_experimental_gin` を有効にする必要があります。

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

また、全文インバーテッドインデックスは重複キーテーブルでのみ作成でき、テーブルプロパティ `replicated_storage` は `false` である必要があります。

#### テーブル作成時に全文インバーテッドインデックスを作成

列 `v` に英語トークン化を使用して全文インバーテッドインデックスを作成します。

```SQL
CREATE TABLE `t` (
  `k` BIGINT NOT NULL COMMENT "",
  `v` STRING COMMENT "",
   INDEX idx (v) USING GIN("parser" = "english")
) ENGINE=OLAP 
DUPLICATE KEY(`k`)
DISTRIBUTED BY HASH(`k`) BUCKETS 1
PROPERTIES (
"replicated_storage" = "false"
);
```

- `parser` パラメータはトークン化方法を指定します。サポートされている値と説明は以下の通りです:
  - `none` (デフォルト): トークン化なし。全文インバーテッドインデックスが構築される際、インデックス化された列のデータ全体が単一のインデックス項目として扱われます。
  - `english`: 英語トークン化。このトークン化方法は通常、非アルファベット文字でトークン化します。また、大文字の英語文字は小文字に変換されます。したがって、クエリ条件のキーワードは大文字の英語ではなく小文字の英語である必要があります。
  - `chinese`: 中国語トークン化。このトークン化方法は、CLucene の [CJK Analyzer](https://lucene.apache.org/core/6_6_1/analyzers-common/org/apache/lucene/analysis/cjk/package-summary.html) を使用します。
  - `standard`: 多言語トークン化。このトークン化方法は、文法に基づくトークン化（[Unicode Text Segmentation algorithm](https://unicode.org/reports/tr29/) に基づく）を提供し、ほとんどの言語や混合言語のケースに適しています。例えば、このトークン化方法は、中国語と英語が共存する場合にこれらの言語を区別できます。英語をトークン化した後、大文字の英語文字を小文字に変換します。したがって、クエリ条件のキーワードは大文字の英語ではなく小文字の英語である必要があります。
- インデックス化された列のデータ型は CHAR、VARCHAR、または STRING でなければなりません。

#### テーブル作成後に全文インバーテッドインデックスを追加

テーブル作成後、`ALTER TABLE ADD INDEX` または `CREATE INDEX` を使用して全文インバーテッドインデックスを追加できます。

```SQL
ALTER TABLE t ADD INDEX idx (v) USING GIN('parser' = 'english');
CREATE INDEX idx ON t (v) USING GIN('parser' = 'english');
```

### 全文インバーテッドインデックスの管理

#### 全文インバーテッドインデックスの表示

`SHOW CREATE TABLE` を実行して全文インバーテッドインデックスを表示します。

```SQL
MySQL [example_db]> SHOW CREATE TABLE t\G
```

#### 全文インバーテッドインデックスの削除

`ALTER TABLE ADD INDEX` または `DROP INDEX` を実行して全文インバーテッドインデックスを削除します。

```SQL
DROP INDEX index idx on t;
ALTER TABLE t DROP index idx;
```

### 全文インバーテッドインデックスによるクエリの高速化

全文インバーテッドインデックスを作成した後、システム変数 `enable_gin_filter` が有効になっていることを確認する必要があります。これにより、インバーテッドインデックスがクエリを高速化できます。また、インデックス列の値がトークン化されているかどうかを考慮して、どのクエリが高速化できるかを判断する必要があります。

#### インデックス化された列がトークン化されている場合のサポートされるクエリ

全文インバーテッドインデックスがインデックス化された列をトークン化する場合、つまり `'parser' = 'standard|english|chinese'` の場合、データフィルタリングに全文インバーテッドインデックスを使用するために `MATCH` プレディケートのみがサポートされ、形式は `<col_name> (NOT) MATCH '%keyword%'` である必要があります。`keyword` は文字列リテラルでなければならず、式はサポートされません。

1. テーブルを作成し、いくつかのテストデータを挿入します。

      ```SQL
      CREATE TABLE `t` (
          `id1` bigint(20) NOT NULL COMMENT "",
          `value` varchar(255) NOT NULL COMMENT "",
          INDEX gin_english (`value`) USING GIN ("parser" = "english") COMMENT 'english index'
      ) 
      DUPLICATE KEY(`id1`)
      DISTRIBUTED BY HASH(`id1`)
      PROPERTIES (
      "replicated_storage" = "false"
      );
      
      
      INSERT INTO t VALUES
          (1, "starrocks is a database
      
      1"),
          (2, "starrocks is a data warehouse");
      ```

2. `MATCH` プレディケートを使用してクエリを行います。

- `value` 列にキーワード `starrocks` を含むデータ行をクエリします。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "starrocks";
    ```

- `value` 列に `data` で始まるキーワードを含むデータ行を取得します。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "data%";
    ```

**注意:**

- クエリ中、キーワードは `%` を使用してあいまいに一致させることができます。形式は `%keyword%` です。ただし、キーワードは単語の一部を含んでいる必要があります。例えば、キーワードが <code>starrocks&nbsp;</code> の場合、スペースを含むため、単語 `starrocks` に一致しません。

    ```SQL
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "star%";
    +------+-------------------------------+
    | id1  | value                         |
    +------+-------------------------------+
    |    1 | starrocks is a database1      |
    |    2 | starrocks is a data warehouse |
    +------+-------------------------------+
    2 rows in set (0.02 sec)
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "starrocks ";
    Empty set (0.02 sec)
    ```

- 英語または多言語トークン化を使用して全文インバーテッドインデックスを構築する場合、実際に全文インバーテッドインデックスが保存される際に、大文字の英語単語は小文字に変換されます。したがって、クエリ中、キーワードは大文字ではなく小文字である必要があります。

    ```SQL
    MySQL [example_db]> INSERT INTO t VALUES (3, "StarRocks is the BEST");
    
    MySQL [example_db]> SELECT * FROM t;
    +------+-------------------------------+
    | id1  | value                         |
    +------+-------------------------------+
    |    1 | starrocks is a database       |
    |    2 | starrocks is a data warehouse |
    |    3 | StarRocks is the BEST         |
    +------+-------------------------------+
    3 rows in set (0.02 sec)
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "BEST"; -- キーワードは大文字の英語
    Empty set (0.02 sec) -- 空の結果セットを返します
    
    MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "best"; -- キーワードは小文字の英語
    +------+-----------------------+
    | id1  | value                 |
    +------+-----------------------+
    |    3 | StarRocks is the BEST | -- 条件を満たすデータ行を特定できます
    +------+-----------------------+
    1 row in set (0.01 sec)
    ```

- クエリ条件の `MATCH` プレディケートはプッシュダウンプレディケートとして使用されなければならないため、WHERE 句にあり、インデックス化された列に対して実行されなければなりません。

    次のテーブルとテストデータを例にとります。

    ```SQL
    CREATE TABLE `t_match` (
        `id1` bigint(20) NOT NULL COMMENT "",
        `value` varchar(255) NOT NULL COMMENT "",
        `value_test` varchar(255) NOT NULL COMMENT "",
        INDEX gin_english (`value`) USING GIN("parser" = "english") COMMENT 'english index'
    )
    ENGINE=OLAP 
    DUPLICATE KEY(`id1`)
    DISTRIBUTED BY HASH (`id1`) BUCKETS 1 
    PROPERTIES (
    "replicated_storage" = "false"
    );
    
    INSERT INTO t_match VALUES (1, "test", "test");
    ```

    次のクエリ文は要件を満たしていません:

    - クエリ文の `MATCH` プレディケートが WHERE 句にないため、プッシュダウンできず、クエリエラーが発生します。

        ```SQL
        MySQL [test]> SELECT value MATCH "test" FROM t_match;
        ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
        ```

    - クエリ文の `MATCH` プレディケートが実行される列 `value_test` がインデックス化された列でないため、クエリが失敗します。

        ```SQL
        MySQL [test]> SELECT * FROM t_match WHERE value_test match "test";
        ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
        ```

#### インデックス化された列がトークン化されていない場合のサポートされるクエリ

全文インバーテッドインデックスがインデックス化された列をトークン化しない場合、つまり `'parser' = 'none'` の場合、以下にリストされているクエリ条件のすべてのプッシュダウンプレディケートを使用して、全文インバーテッドインデックスを使用してデータをフィルタリングできます:

- 式プレディケート: (NOT) LIKE, (NOT) MATCH
  
  :::note

  - この場合、`MATCH` は意味的に `LIKE` と同等です。
  - `MATCH` と `LIKE` は、形式 `(NOT) <col_name> MATCH|LIKE '%keyword%'` のみをサポートします。`keyword` は文字列リテラルであり、式はサポートされません。注意: `LIKE` がこの形式を満たさない場合、クエリが正常に実行できたとしても、全文インバーテッドインデックスを使用してデータをフィルタリングしないクエリに劣化します。
  :::
- 通常のプレディケート: `==`, `!=`, `<=`, `>=`, `NOT IN`, `IN`, `IS NOT NULL`, `NOT NULL`

## 全文インバーテッドインデックスがクエリを高速化するかどうかを確認する方法

クエリを実行した後、Query Profile のスキャンノードで詳細なメトリクス `GinFilterRows` と `GinFilter` を確認することで、全文インバーテッドインデックスを使用してフィルタリングされた行数とフィルタリング時間を確認できます。