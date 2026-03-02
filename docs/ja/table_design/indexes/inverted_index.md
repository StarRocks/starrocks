---
displayed_sidebar: docs
toc_max_heading_level: 4
sidebar_position: 50
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# 全文転置インデックス

<Beta />

バージョン3.3.0以降、StarRocksは全文転置インデックスをサポートしています。これは、テキストをより小さな単語に分割し、各単語について、その単語とデータファイル内の対応する行番号とのマッピング関係を示すインデックスエントリを作成できます。全文検索の場合、StarRocksは検索キーワードに基づいて転置インデックスをクエリし、キーワードに一致するデータ行を迅速に特定します。

プライマリキーテーブルは、v4.0以降、全文転置インデックスをサポートしています。

v4.1以降、StarRocksは**組み込みの**転置インデックス実装を、デフォルトのCLuceneベースの実装に加えてサポートしています。組み込み実装は、**シェアードナッシング**と**シェアードデータ**クラスターの両方をサポートしています。詳細については、[組み込み転置インデックス](#built-in-inverted-index)を参照してください。

## 概要

StarRocksは、基盤となるデータを列ごとに整理されたデータファイルに保存します。各データファイルには、インデックス付き列に基づいた全文転置インデックスが含まれています。インデックス付き列の値は個々の単語にトークン化されます。トークン化後の各単語はインデックスエントリとして扱われ、その単語が出現する行番号にマッピングされます。現在サポートされているトークン化メソッドは、英語トークン化、中国語トークン化、多言語トークン化、およびトークン化なしです。

例えば、データ行に「hello world」が含まれ、その行番号が123の場合、全文転置インデックスはこのトークン化結果と行番号に基づいてインデックスエントリを構築します: hello->123, world->123。

全文検索中、StarRocksは全文転置インデックスを使用して検索キーワードを含むインデックスエントリを特定し、キーワードが出現する行番号を迅速に見つけることで、スキャンする必要があるデータ行の数を大幅に削減できます。

## 基本操作

### 全文転置インデックスの作成

全文転置インデックスを作成する前に、FE構成項目`enable_experimental_gin`を有効にする必要があります。

```sql
ADMIN SET FRONTEND CONFIG ("enable_experimental_gin" = "true");
```

:::note
テーブルに全文転置インデックスを作成する場合、`replicated_storage`機能を無効にする必要があります。

- v4.0以降では、インデックス作成時にこの機能は自動的に無効になります。
- v4.0より前のバージョンでは、テーブルプロパティ`replicated_storage`を`false`に手動で設定する必要があります。
:::

#### テーブル作成時に全文転置インデックスを作成する

列`v`に英語トークン化で全文転置インデックスを作成します。

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

- パラメーターはトークン化メソッドを指定します。サポートされている値と説明は以下のとおりです。`parser` (デフォルト): トークン化なし。全文転置インデックスが構築される際、インデックス付き列のデータ行全体が単一のインデックス項目として扱われます。
  - `none`: 英語トークン化。このトークン化メソッドは通常、非アルファベット文字でトークン化します。また、大文字の英字は小文字に変換されます。したがって、全文転置インデックスを活用してデータ行を特定するには、クエリ条件のキーワードは大文字の英語ではなく小文字の英語である必要があります。
  - `english`: 中国語トークン化。このトークン化メソッドは、
  - `chinese`CJKアナライザー[をCLuceneで使用してトークン化します。](https://lucene.apache.org/core/6_6_1/analyzers-common/org/apache/lucene/analysis/cjk/package-summary.html): 多言語トークン化。このトークン化メソッドは、文法に基づいたトークン化（
  - `standard`Unicodeテキストセグメンテーションアルゴリズム[に基づく）を提供し、ほとんどの言語や中国語と英語のような混合言語の場合にうまく機能します。例えば、このトークン化メソッドは、中国語と英語が共存する場合に両者を区別できます。英語をトークン化した後、大文字の英字を小文字に変換します。したがって、全文転置インデックスを活用してデータ行を特定するには、クエリ条件のキーワードは大文字の英語ではなく小文字の英語である必要があります。](https://unicode.org/reports/tr29/)インデックス付き列のデータ型は、CHAR、VARCHAR、またはSTRINGである必要があります。
- インデックス付き列のデータ型は、CHAR、VARCHAR、または STRING である必要があります。

#### テーブル作成後に全文転置インデックスを追加する

テーブル作成後、全文転置インデックスを以下の方法で追加できます。`ALTER TABLE ADD INDEX`または`CREATE INDEX`。

```SQL
ALTER TABLE t ADD INDEX idx (v) USING GIN('parser' = 'english');
CREATE INDEX idx ON t (v) USING GIN('parser' = 'english');
```

### 全文転置インデックスの管理

#### 全文転置インデックスの表示

を実行して`SHOW CREATE TABLE`全文転置インデックスを表示します。

```SQL
MySQL [example_db]> SHOW CREATE TABLE t\G
```

#### 全文転置インデックスの削除

を実行するか、`ALTER TABLE ADD INDEX`または`DROP INDEX`を実行して全文転置インデックスを削除します。

```SQL
DROP INDEX idx on t;
ALTER TABLE t DROP index idx;
```

### 全文転置インデックスによるクエリの高速化

全文転置インデックスを作成した後、システム変数`enable_gin_filter`が有効になっていることを確認する必要があります。これにより、転置インデックスがクエリを高速化できます。また、インデックス列の値がトークン化されているかどうかを考慮して、どのクエリが高速化できるかを判断する必要があります。

#### インデックス列がトークン化されている場合にサポートされるクエリ

全文転置インデックス列がトークン化（`parser`=`standard`|`english`|`chinese`）で有効になっている場合、`MATCH`、`MATCH_ANY`、または`MATCH_ALL`述語のみがフィルタリングにサポートされます。サポートされる形式は次のとおりです。

- `<col_name> (NOT) MATCH '%keyword%'`
- `<col_name> (NOT) MATCH_ANY 'keyword1, keyword2'`
- `<col_name> (NOT) MATCH_ALL 'keyword1, keyword2'`

ここで、キーワードは文字列リテラルである必要があります。式はサポートされていません。

1. テーブルを作成し、テストデータを数行挿入します。

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

2. 述語を使用して`MATCH`クエリを実行します。

- 列にキーワード`value`が含まれるデータ行をクエリします。`starrocks`。

  ```SQL
  MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "starrocks";
  ```

- 列に`value`で始まるキーワードが含まれるデータ行を取得します。`data`。

  ```SQL
  MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "data%";
  ```

3. 述語を使用して`MATCH_ANY`クエリを実行します。

- 列にキーワード`value`または`database`が含まれるデータ行をクエリします。`data`。

  ```SQL
  MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH_ANY "database data";
  ```

4. 「`MATCH_ALL`」述語を使用してクエリを実行します。

- 「`value`」列にキーワード「`database`」と「`data`」の両方が含まれているデータ行をクエリします。

  ```SQL
  MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH_ALL "database data";
  ```

**注記:**

- クエリ中、キーワードは「`%`」を使用してあいまい一致させることができます。形式は「`%keyword%`」です。ただし、キーワードは単語の一部を含んでいる必要があります。例えば、キーワードが「<code>starrocks </code>」の場合、スペースが含まれているため、単語「`starrocks`」とは一致しません。

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

- 全文転置インデックスの構築に英語または多言語のトークン化が使用されている場合、全文転置インデックスが実際に保存される際には、大文字の英単語は小文字に変換されます。したがって、クエリ時には、全文転置インデックスを利用してデータ行を特定するために、キーワードは大文字ではなく小文字にする必要があります。

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

  MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "BEST"; -- Keyword is uppercase English
  Empty set (0.02 sec) -- Returns an empty result set

  MySQL [example_db]> SELECT * FROM t WHERE t.value MATCH "best"; -- Keyword is lowercase English
  +------+-----------------------+
  | id1  | value                 |
  +------+-----------------------+
  |    3 | StarRocks is the BEST | -- Can locate data rows that meet the condition
  +------+-----------------------+
  1 row in set (0.01 sec)
  ```

- クエリ条件の「`MATCH`」、「`MATCH_ANY`」、または「`MATCH_ALL`」述語はプッシュダウン述語として使用する必要があるため、WHERE句にあり、インデックス付き列に対して実行される必要があります。

  以下のテーブルとテストデータを例にとります。

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

  以下のクエリステートメントは要件を満たしていません。

  - クエリステートメントの「`MATCH`」、「`MATCH_ANY`」、または「`MATCH_ALL`」述語がWHERE句にないため、プッシュダウンできず、クエリエラーが発生します。

    ```SQL
    MySQL [test]> SELECT value MATCH "test" FROM t_match;
    ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
    ```

  - クエリステートメントの「`value_test`」、「`MATCH`」、「`MATCH_ANY`」、または「`MATCH_ALL`」述語が実行されるのがインデックス付き列ではないため、クエリは失敗します。

    ```SQL
    MySQL [test]> SELECT * FROM t_match WHERE value_test match "test";
    ERROR 1064 (HY000): Match can only be used as a pushdown predicate on a column with GIN in a single query.
    ```

#### インデックス付き列がトークン化されていない場合のサポートされるクエリ

全文転置インデックスがインデックス付き列をトークン化しない場合、つまり「`'parser' = 'none'`」の場合、以下に示すクエリ条件のすべてのプッシュダウン述語は、全文転置インデックスを使用してデータフィルタリングに利用できます。

- 式述語: (NOT) LIKE, (NOT) MATCH, (NOT) MATCH_ANY, (NOT) MATCH_ALL

  :::note

  - この場合、「`MATCH`」は、意味的に「`LIKE`」と同等です。
  - `MATCH`「」と「`LIKE`」は、形式「`(NOT) <col_name> MATCH|LIKE '%keyword%'`」のみをサポートします。`keyword` は文字列リテラルである必要があり、式はサポートしていません。なお、`LIKE` がこの形式を満たさない場合、クエリが正常に実行できたとしても、全文転置インデックスを使用してデータをフィルタリングしないクエリに劣化します。
:::
- 通常の述語: `==`,`!=`,`<=`,`>=`,`NOT IN`,`IN`,`IS NOT NULL`,`NOT NULL`

## クエリが全文転置インデックスによって高速化されているかを確認する

クエリ実行後、詳細なメトリクス `GinFilterRows` および `GinFilter` をクエリプロファイルのScanノードで確認し、全文転置インデックスを使用した行のフィルタリング数とフィルタリング時間を確認できます。

## 組み込み転置インデックス

v4.1以降、StarRocksはデフォルトのCLuceneベースの実装に加えて、**組み込み**転置インデックス実装を提供しています。組み込み実装は、ビットマップインデックスの上に構築されたStarRocksネイティブの転置インデックスであり、**shared-nothing** および **shared-data** クラスターの両方をサポートしています。

:::note
CLuceneベースの転置インデックス実装は、shared-dataクラスターではサポートされていません。shared-dataクラスターでは、組み込み実装を使用する必要があります。
:::

### 実装の比較

| 実装 | サポートバージョン | Shared-nothing | Shared-data | 説明 |
|---------------|----------------|----------------|-------------|-------------|
| **CLucene** (デフォルト) | v3.3.0 | はい | いいえ | CLucene全文検索ライブラリに基づいています。これはshared-nothingクラスターのデフォルト実装です。 |
| **組み込み** | v4.1.0 | はい | はい | StarRocksネイティブの転置インデックス実装です。shared-nothingクラスターとshared-dataクラスターの両方をサポートしています。 |

インデックス作成時に `imp_lib` パラメーターを使用して実装タイプを明示的に指定できます。指定しない場合、システムはクラスターモードに基づいて適切な実装を自動的に選択します。

- shared-nothingクラスターでは、デフォルトはCLuceneです。
- shared-dataクラスターでは、デフォルトは組み込みです（CLuceneはサポートされていません）。

### 組み込み転置インデックスの作成

#### テーブル作成時に作成

```SQL
-- Create table with built-in inverted index
CREATE TABLE `t_builtin` (
    `id1` bigint(20) NOT NULL COMMENT "",
    `value` varchar(255) NOT NULL COMMENT "",
    INDEX gin_english (`value`) USING GIN ("parser" = "english", "imp_lib" = "builtin") COMMENT 'builtin english index'
)
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`)
PROPERTIES (
"replicated_storage" = "false"
);
```

#### テーブル作成後に追加

```SQL
ALTER TABLE t ADD INDEX idx_builtin (v) USING GIN('parser' = 'english', 'imp_lib' = 'builtin');
-- Or
CREATE INDEX idx_builtin ON t (v) USING GIN('parser' = 'english', 'imp_lib' = 'builtin');
```

### shared-dataクラスターにおける組み込み転置インデックス

shared-dataクラスターでは、`imp_lib` が指定されていなくても、組み込み実装が自動的に選択されます。通常の転置インデックスと同じ構文を使用してインデックスを作成できます。

```SQL
-- In a shared-data cluster, this automatically uses the built-in implementation
CREATE TABLE `t_shared_data` (
    `id1` bigint(20) NOT NULL COMMENT "",
    `value` varchar(255) NOT NULL COMMENT "",
    INDEX gin_english (`value`) USING GIN ("parser" = "english") COMMENT 'english index'
)
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`);
```

また、明示的に `"imp_lib" = "builtin"`明確にするために:

```SQL
CREATE TABLE `t_shared_data_explicit` (
    `id1` bigint(20) NOT NULL COMMENT "",
    `value` varchar(255) NOT NULL COMMENT "",
    INDEX gin_english (`value`) USING GIN ("parser" = "english", "imp_lib" = "builtin") COMMENT 'builtin english index'
)
DUPLICATE KEY(`id1`)
DISTRIBUTED BY HASH(`id1`);
```

### dict_gram_num

「`dict_gram_num`」パラメーターは、**組み込みの**転置インデックス実装でのみ利用可能です。これは、転置インデックス辞書の上にn-gram辞書インデックスを構築するために使用されるn-gramサイズを制御し、ワイルドカードクエリや部分文字列クエリを大幅に高速化できます。

#### 仕組み

「`dict_gram_num`」が正の整数（例: `2`）に設定されている場合、組み込みの転置インデックスは、インデックス構築中に各辞書エントリをn-gram（指定された文字長のサブストリング）に分割します。例えば、「`dict_gram_num`」が「`2`」に設定され、辞書エントリが「`"starrocks"`」の場合、以下の2-gramが生成されます: `"st"`、`"ta"`、`"ar"`、`"rr"`、`"ro"`、`"oc"`、`"ck"`、`"ks"`。

「`MATCH '%rock%'`」のようなクエリ中、クエリ文字列「`"rock"`」も2-gram（「`"ro"`、`"oc"`、`"ck"`）に分割され、n-gramインデックスは候補となる辞書エントリを迅速に絞り込み、完全な辞書スキャンを回避します。これにより、特に多数の異なる値を持つ列でのワイルドカードクエリのパフォーマンスが大幅に向上します。

#### パラメーターの詳細

| パラメーター | デフォルト | 有効範囲 | 説明 |
|-----------|---------|-------------|-------------|
| `dict_gram_num` | `-1` (無効) | 正の整数（例: `1`、`2`、`3`、...） | 辞書n-gramインデックスを構築するためのn-gramサイズ。「`imp_lib` = `builtin`」の場合にのみ有効です。|

#### dict_gram_numを選択するためのガイドライン

- 小さい値（例: `2`）は、辞書エントリごとに多くのn-gramを生成し、インデックスサイズを増加させますが、短いクエリパターンに対してより良いフィルタリングを提供します。
- 大きい値（例: `4`）は生成されるn-グラムが少なくなり、インデックスサイズは小さくなりますが、短いクエリパターンに対するフィルタリング効果は低下します。
- クエリが主に短いワイルドカードパターン（例：`%ab%`）を使用する場合、より小さい「`dict_gram_num`」の値（例：「`2`」）が推奨されます。
- クエリパターンが「`dict_gram_num`」の値よりも短い場合、そのクエリにはn-グラムインデックスを使用できず、完全な辞書スキャンにフォールバックします。

#### 例

**組み込みの転置インデックスを「`dict_gram_num`**

1. 組み込みの転置インデックスと「`dict_gram_num`」を「`2`」に設定してテーブルを作成します。

   ```SQL
   CREATE TABLE `t_gram` (
       `id1` bigint(20) NOT NULL COMMENT "",
       `text_val` varchar(255) NOT NULL COMMENT "",
       INDEX idx_gram (`text_val`) USING GIN (
           "parser" = "english",
           "imp_lib" = "builtin",
           "dict_gram_num" = "2"
       ) COMMENT 'builtin index with ngram'
   )
   DUPLICATE KEY(`id1`)
   DISTRIBUTED BY HASH(`id1`)
   PROPERTIES (
   "replicated_storage" = "false"
   );
   ```

2. テストデータを挿入します。

   ```SQL
   INSERT INTO t_gram VALUES
       (1, "starrocks is a high performance database"),
       (2, "apache spark is a data processing engine"),
       (3, "rocksdb is an embedded key value store");
   ```

3. ワイルドカードパターンでクエリを実行します。n-グラム辞書インデックスはこれらのクエリを高速化します。

   ```SQL
   -- Query for rows containing "rock" in the text
   MySQL [example_db]> SELECT * FROM t_gram WHERE text_val MATCH "%rock%";
   +------+--------------------------------------------+
   | id1  | text_val                                   |
   +------+--------------------------------------------+
   |    1 | starrocks is a high performance database   |
   |    3 | rocksdb is an embedded key value store      |
   +------+--------------------------------------------+
   2 rows in set (0.01 sec)
   ```

**「`dict_gram_num`」を既存のテーブルに追加する**

テーブル作成後に組み込みの転置インデックスを追加する際に、「`dict_gram_num`」を指定することもできます。

```SQL
CREATE INDEX idx_gram ON t (v) USING GIN('parser' = 'english', 'imp_lib' = 'builtin', 'dict_gram_num' = '3');
```
