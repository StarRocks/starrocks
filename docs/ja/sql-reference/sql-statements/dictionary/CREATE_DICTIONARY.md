---
displayed_sidebar: docs
---

import Beta from '../../../_assets/commonMarkdown/_beta.mdx'

# CREATE DICTIONARY

<Beta />

元のオブジェクトに基づいて辞書オブジェクトを作成します。辞書オブジェクトは、元のオブジェクトからのキーと値のマッピングをハッシュテーブルの形式で整理し、すべての BE ノードのメモリにキャッシュされます。キャッシュされたテーブルとして見ることができます。

**利点**

- **辞書オブジェクトのための豊富な元オブジェクト**: `dictionary_get()` を使用して辞書オブジェクトをクエリする場合、元のオブジェクトは任意のタイプのテーブル、非同期マテリアライズドビュー、またはビューであることができます。ただし、`dict_mapping()` を使用して辞書テーブルをクエリする場合、辞書テーブルは主キーテーブルのみである必要があります。
- **高速なクエリ速度**: 辞書オブジェクトはハッシュテーブルであり、すべての BE ノードのメモリに完全にキャッシュされているため、辞書オブジェクトをクエリしてマッピングを取得することは、メモリ内のハッシュテーブルを参照することによって実現されます。したがって、クエリ速度は非常に速いです。
- **複数の値カラムをサポート**: 内部的に、辞書オブジェクトは複数の値カラムを単一の STRUCT タイプのカラムにエンコードします。キーに基づくクエリでは、複数の値が一緒に返されます。したがって、辞書オブジェクトは、各キー（通常は一意の識別子）が複数の値（説明的な属性）に対応するディメンションテーブルとして機能できます。
- **一貫したスナップショット読み取りを保証**: 同じトランザクション内で取得された辞書スナップショットは一貫しており、同じクエリまたはロードプロセス中に辞書オブジェクトからのクエリ結果が変わらないことを保証します。

## 構文

```SQL
CREATE DICTIONARY <dictionary_object_name> USING <dictionary_source>
(
    column_name KEY, [..., column_name KEY,]
    column_name VALUE[, ..., column_name VALUE]
)
[PROPERTIES ("key"="value", ...)];
```

## パラメータ

- `dictionary_object_name`: 辞書オブジェクトの名前。辞書オブジェクトはグローバルに有効であり、特定のデータベースに属しません。
- `dictionary_source`: 辞書オブジェクトが基づく元のオブジェクトの名前。元のオブジェクトは任意のタイプのテーブル、非同期マテリアライズドビュー、またはビューであることができます。
- 辞書オブジェクトのカラムの定義: 辞書テーブルで維持されるキーと値のマッピングを保持するために、辞書オブジェクトのカラムで `KEY` と `VALUE` キーワードを使用してキーとそのマッピングされた値を指定する必要があります。
  - 辞書オブジェクト内のカラム名 `column_name` は、辞書テーブル内のものと一致している必要があります。
  - 辞書オブジェクト内のキーと値のカラムのデータ型は、ブール型、整数型、文字列型、日付型に限定されます。
  - 元のオブジェクト内のキーのカラムは、一意性を保証する必要があります。
- 辞書オブジェクトの関連プロパティ (`PROPERTIES`):
  - `dictionary_warm_up`: 各 BE ノードにデータをキャッシュする方法。有効な値: `TRUE`（デフォルト）または `FALSE`。パラメータが `TRUE` に設定されている場合、辞書オブジェクトの作成後にデータが自動的にキャッシュされます。`FALSE` に設定されている場合、データをキャッシュするために辞書オブジェクトを手動で更新する必要があります。
  - `dictionary_memory_limit`: 各 BE ノードで辞書オブジェクトが占有できる最大メモリ。単位: バイト。デフォルト値: 2,000,000,000 バイト（2 GB）。
  - `dictionary_refresh_interval`: 辞書オブジェクトを定期的に更新する間隔。単位: 秒。デフォルト値: `0`。値が `<=0` の場合、自動更新はありません。
  - `dictionary_read_latest`: 主に更新中にクエリされた辞書オブジェクトに影響を与える、最新の辞書オブジェクトのみをクエリするかどうか。有効な値: `TRUE` または `FALSE`（デフォルト）。パラメータが `TRUE` に設定されている場合、最新の辞書オブジェクトがまだ更新中であるため、更新中に辞書オブジェクトをクエリすることはできません。`FALSE` に設定されている場合、以前に正常にキャッシュされた辞書オブジェクトを更新中にクエリできます。
  - `dictionary_ignore_failed_refresh`: 更新が失敗した場合に、最後に正常にキャッシュされた辞書オブジェクトに自動的にロールバックするかどうか。有効な値: `TRUE` または `FALSE`（デフォルト）。パラメータが `TRUE` に設定されている場合、更新が失敗したときに最後に正常にキャッシュされた辞書オブジェクトに自動的にロールバックします。`FALSE` に設定されている場合、更新が失敗したときに辞書オブジェクトのステータスは `CANCELLED` に設定されます。

## 使用上の注意

- 辞書オブジェクトは各 BE ノードのメモリに完全にキャッシュされるため、比較的多くのメモリを消費します。
- 元のオブジェクトが削除されても、それに基づいて作成された辞書オブジェクトは依然として存在します。辞書オブジェクトを手動で DROP する必要があります。

## 例

**例 1: 元の辞書テーブルを置き換えるためのシンプルな辞書オブジェクトを作成する。**

次の辞書テーブルを例に取り、テストデータを挿入します。

```Plain
MySQL > CREATE TABLE dict (
    order_uuid STRING,
    order_id_int BIGINT AUTO_INCREMENT 
)
PRIMARY KEY (order_uuid)
DISTRIBUTED BY HASH (order_uuid);
Query OK, 0 rows affected (0.02 sec)
MySQL > INSERT INTO dict (order_uuid) VALUES ('a1'), ('a2'), ('a3');
Query OK, 3 rows affected (0.12 sec)
{'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
MySQL > SELECT * FROM dict;
+------------+--------------+
| order_uuid | order_id_int |
+------------+--------------+
| a1         |            1 |
| a2         |            2 |
| a3         |            3 |
+------------+--------------+
3 rows in set (0.01 sec)
```

この辞書テーブルのマッピングに基づいて辞書オブジェクトを作成します。

```Plain
MySQL > CREATE DICTIONARY dict_obj USING dict
    (order_uuid KEY,
     order_id_int VALUE);
Query OK, 0 rows affected (0.00 sec)
```

将来の辞書テーブルのマッピングのクエリでは、辞書テーブルではなく辞書オブジェクトを直接クエリできます。例えば、キー `a1` にマッピングされた値をクエリします。

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```

**例 2: 元のディメンションテーブルを置き換えるための辞書オブジェクトを作成する**

次のディメンションテーブルを例に取り、テストデータを挿入します。

```Plain
MySQL > CREATE TABLE ProductDimension (
    ProductKey BIGINT AUTO_INCREMENT,
    ProductName VARCHAR(100) NOT NULL,
    Category VARCHAR(50),
    SubCategory VARCHAR(50),
    Brand VARCHAR(50),
    Color VARCHAR(20),
    Size VARCHAR(20)
)
PRIMARY KEY (ProductKey)
DISTRIBUTED BY HASH (ProductKey);
MySQL > INSERT INTO ProductDimension (ProductName, Category, SubCategory, Brand, Color, Size)
VALUES
    ('T-Shirt', 'Apparel', 'Shirts', 'BrandA', 'Red', 'M'),
    ('Jeans', 'Apparel', 'Pants', 'BrandB', 'Blue', 'L'),
    ('Running Shoes', 'Footwear', 'Athletic', 'BrandC', 'Black', '10'),
    ('Jacket', 'Apparel', 'Outerwear', 'BrandA', 'Green', 'XL'),
    ('Baseball Cap', 'Accessories', 'Hats', 'BrandD', 'White', 'OneSize');
Query OK, 5 rows affected (0.48 sec)
{'label':'insert_e938481f-181e-11ef-a6a9-00163e19e14e', 'status':'VISIBLE', 'txnId':'50'}
MySQL > SELECT * FROM ProductDimension;
+------------+---------------+-------------+-------------+--------+-------+---------+
| ProductKey | ProductName   | Category    | SubCategory | Brand  | Color | Size    |
+------------+---------------+-------------+-------------+--------+-------+---------+
|          1 | T-Shirt       | Apparel     | Shirts      | BrandA | Red   | M       |
|          2 | Jeans         | Apparel     | Pants       | BrandB | Blue  | L       |
|          3 | Running Shoes | Footwear    | Athletic    | BrandC | Black | 10      |
|          4 | Jacket        | Apparel     | Outerwear   | BrandA | Green | XL      |
|          5 | Baseball Cap  | Accessories | Hats        | BrandD | White | OneSize |
+------------+---------------+-------------+-------------+--------+-------+---------+
5 rows in set (0.02 sec)
```

元のディメンションテーブルを置き換えるために辞書オブジェクトを作成します。

```Plain
MySQL > CREATE DICTIONARY dimension_obj USING ProductDimension 
    (ProductKey KEY,
     ProductName VALUE,
     Category VALUE,
     SubCategory VALUE,
     Brand VALUE,
     Color VALUE,
     Size VALUE);
Query OK, 0 rows affected (0.00 sec)
```

将来のディメンション値のクエリでは、ディメンションテーブルではなく辞書オブジェクトを直接クエリしてディメンション値を取得できます。例えば、キー `1` にマッピングされた値をクエリします。

```Plain
MySQL > SELECT dictionary_get("dict_obj", "a1");
+--------------------+
| DICTIONARY_GET     |
+--------------------+
| {"order_id_int":1} |
+--------------------+
1 row in set (0.01 sec)
```