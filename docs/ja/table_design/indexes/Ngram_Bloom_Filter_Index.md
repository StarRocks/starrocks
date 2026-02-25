---
displayed_sidebar: docs
sidebar_position: 40
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# N-gram ブルームフィルターインデックス

<Beta />

N-gram ブルームフィルターインデックスは、通常、LIKE クエリや `ngram_search` および `ngram_search_case_insensitive` 関数の計算速度を向上させるために使用される特別な [ブルームフィルターインデックス](./Bloomfilter_index.md) です。

N-gram ブルームフィルターインデックスは、文字列型（`STRING`、`CHAR`、または `VARCHAR`）の列にのみ適しています。N-gram ブルームフィルターインデックスとブルームフィルターインデックスの違いは、N-gram ブルームフィルターインデックスが最初に文字列をトークン化し、その結果得られたサブストリングをブルームフィルターインデックスに書き込むことです。たとえば、あるインデックス付き列の値が文字列 `Technical` であるとします。従来のブルームフィルターインデックスでは、文字列 `Technical` 全体が直接インデックスに書き込まれます。しかし、`gram_num` が `4` に指定された N-gram ブルームフィルターインデックスでは、文字列 `Technical` は次のようなサブストリングにトークン化されます。

```sql
"Tech", "echn", "chni", "hnic", "nica", "ical"
```

そして、これらの6つのサブストリングがブルームフィルターインデックスに書き込まれます。

## 使用上の注意

- Duplicate Key または主キーテーブルの場合、すべての列（文字列型）に対して N-gram ブルームフィルターインデックスを作成できます。集計テーブルまたはユニークキーテーブルの場合、キー列（文字列型）にのみ N-gram ブルームフィルターインデックスを作成できます。
- N-gram ブルームフィルターインデックスは、文字列型（CHAR、STRING、VARCHAR）の列にのみ作成できます。
- クエリが N-gram ブルームフィルターインデックスにヒットしたかどうかを確認するには、クエリのプロファイル内の `BloomFilterFilterRows` フィールドをチェックします。
- 単一の列に対して作成できるインデックスの種類は、ブルームフィルターまたは N-gram ブルームフィルターのいずれか一つです。

## 基本操作

### N-gram ブルームフィルターインデックスの作成

```SQL
CREATE TABLE test.table1
(
    k1 CHAR(10),
    k2 CHAR(10),
    v1 INT SUM,
    INDEX index_name (k2) USING NGRAMBF ("gram_num" = "4",
                                         "bloom_filter_fpp" = "0.05") COMMENT ''
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1);

```

N-gram ブルームフィルターインデックス関連のパラメータ:

| **Parameter**    | **Required** | **Description**                                              |
| ---------------- | ------------ | ------------------------------------------------------------ |
| `index_name`       | Yes          | インデックスの名前。インデックス名はテーブル内で一意でなければなりません。 |
| `column_name`      | Yes          | インデックスが作成される列の名前。指定できるのは単一の列名のみです。上記の例では `k2` です。 |
| `gram_num`         | No           | インデックス付き列の文字列がトークン化された後のサブストリングの長さ。デフォルト値は2です。 |
| `bloom_filter_fpp` | No           | ブルームフィルターの偽陽性の可能性で、0.0001から0.05の範囲です。デフォルト値は0.05です。小さい値はより良いフィルタリングを提供しますが、より大きなストレージオーバーヘッドを伴います。 |
| `case_sensitive`   | No           | このインデックスが大文字小文字を区別するかどうか。デフォルト値は `true` です。 |
| `COMMENT`          | No           | インデックスコメント。 |

テーブル作成に関連する他のパラメータの説明については、 [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

### N-gram ブルームフィルターインデックスの表示

`SHOW CREATE TABLE` または `SHOW INDEX FROM` テーブルを使用して、テーブルのすべてのインデックスを表示できます。インデックスの作成は非同期であるため、インデックスが正常に作成された後にのみ対応するインデックスを確認できます。

```SQL
SHOW CREATE TABLE table1;
SHOW INDEX FROM table1;
```

### N-gram ブルームフィルターインデックスの変更

[ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) ステートメントを使用して、N-gram ブルームフィルターインデックスを追加および削除できます。

- 次のステートメントを実行して、テーブル `table1` の列 `k1` に新しい N-gram ブルームフィルターインデックス `new_index_name` を追加します。

  ```SQL
  ALTER TABLE table1 
  ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                              "bloom_filter_fpp" = "0.05") COMMENT '';
  ```

- 次のステートメントを実行して、テーブル `table1` から N-gram ブルームフィルターインデックス `new_index_name` を削除します。

  ```SQL
  ALTER TABLE table1 DROP INDEX new_index_name;
  ```

:::note

インデックスの変更は非同期操作です。この操作の進行状況を確認するには、 [SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md) を実行します。テーブル内のインデックスを変更するタスクは一度に1つしか実行できません。

:::

## 加速可能なクエリ

### `LIKE` クエリ

N-gram ブルームフィルターインデックスは、`gram_num` が十分に小さい場合（クエリされる文字列の長さより小さい場合）、`LIKE` クエリを加速できます。それ以外の場合、N-gram ブルームフィルターインデックスは `LIKE` クエリを加速できません。

たとえば、`gram_num` が `4` の場合、クエリステートメントが `SELECT * FROM table WHERE col1 LIKE "%abc"` であると、クエリされる文字列が `abc` であり、3文字しかないため、N-gram ブルームフィルターインデックスはこのクエリを加速しません。しかし、クエリ条件が `WHERE col1 LIKE "%abcd"` または `WHERE col1 LIKE "%abcde%"` の場合、N-gram ブルームフィルターインデックスはクエリを加速します。

### ngram_search

クエリで `ngram_search` 関数が使用される場合、関数で指定された列に N-gram ブルームフィルターインデックスがあり、関数で指定された `gram_num` が N-gram ブルームフィルターインデックスの `gram_num` と一致する場合、インデックスは自動的に文字列類似度が0のデータをフィルタリングし、関数の実行プロセスを大幅に高速化します。

### ngram_search_case_insensitive

この関数の使用法は `ngram_search` と同じですが、この関数は大文字小文字を区別します。そのため、N-gram ブルームフィルターインデックスを作成する際に `case_sensitive` を `false` にする必要があります。

```SQL
CREATE TABLE test.table1
(
    k1 CHAR(10),
    k2 CHAR(10),
    v1 INT SUM,
    INDEX index_name (k2) USING NGRAMBF ("gram_num" = "4",
                                         "bloom_filter_fpp" = "0.05",
                                         "case_sensitive" = "false") COMMENT ''
)
ENGINE = olap
AGGREGATE KEY(k1, k2)
DISTRIBUTED BY HASH(k1);
```

インデックスがすでに作成されており、そのパラメータ `case_sensitive` が `true` に設定されている場合、次のようにしてこのインデックスのパラメータを `false` に変更できます。

```SQL
ALTER TABLE table1 
ADD INDEX new_index_name(k1) USING NGRAMBF ("gram_num" = "4", 
                                            "bloom_filter_fpp" = "0.05",
                                            "case_sensitive" = "false") COMMENT '';
```