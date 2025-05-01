---
displayed_sidebar: docs
---

# ソートキーとプレフィックスインデックス

テーブルを作成する際、ソートキーを構成するために1つ以上のカラムを選択できます。ソートキーは、テーブルのデータがディスクに保存される前にソートされる順序を決定します。ソートキーのカラムをクエリのフィルター条件として使用することができます。これにより、StarRocks は必要なデータをすばやく特定し、テーブル全体をスキャンすることなく処理に必要なデータを見つけることができます。これにより、検索の複雑さが軽減され、クエリが高速化されます。

さらに、メモリ消費を削減するために、StarRocks はテーブルにプレフィックスインデックスを作成することをサポートしています。プレフィックスインデックスは、スパースインデックスの一種です。StarRocks はテーブルの1024行ごとにブロックに保存し、そのためのインデックスエントリを生成してプレフィックスインデックステーブルに保存します。ブロックのプレフィックスインデックスエントリは36バイトを超えてはならず、その内容はそのブロックの最初の行のテーブルのソートキーカラムで構成されるプレフィックスです。これにより、プレフィックスインデックステーブルで検索が実行されたときに、その行のデータを保存しているブロックの開始カラム番号をすばやく特定することができます。テーブルのプレフィックスインデックスは、テーブル自体のサイズの1024分の1です。したがって、プレフィックスインデックス全体をメモリにキャッシュしてクエリを高速化することができます。

## 原則

重複キーテーブルでは、ソートキーカラムは `DUPLICATE KEY` キーワードを使用して定義されます。

集計テーブルでは、ソートキーカラムは `AGGREGATE KEY` キーワードを使用して定義されます。

ユニークキーテーブルでは、ソートキーカラムは `UNIQUE KEY` キーワードを使用して定義されます。

主キーテーブルでは、ソートキーカラムは主キーカラムであり、`PRIMARY KEY` キーワードを使用して定義されます。

テーブル作成時にソートキーカラムを定義する際には、次の点に注意してください：

- ソートキーカラムは連続して定義されたカラムでなければならず、最初に定義されたカラムが開始ソートキーカラムでなければなりません。

- ソートキーカラムとして選択する予定のカラムは、他の一般的なカラムよりも前に定義されている必要があります。

- ソートキーカラムをリストする順序は、テーブルのカラムを定義する順序に従わなければなりません。

次の例は、4つのカラム `site_id`、`city_code`、`user_id`、`pv` からなるテーブルの許可されたソートキーカラムと許可されていないソートキーカラムを示しています：

- 許可されたソートキーカラムの例
  - `site_id` と `city_code`
  - `site_id`、`city_code`、`user_id`

- 許可されていないソートキーカラムの例
  - `city_code` と `site_id`
  - `city_code` と `user_id`
  - `site_id`、`city_code`、`pv`

次のセクションでは、異なるタイプのテーブルを作成する際にソートキーカラムを定義する方法の例を示します。これらの例は、少なくとも3つの BE を持つ StarRocks クラスターに適しています。

### Duplicate Key

`site_access_duplicate` という名前のテーブルを作成します。このテーブルは、`site_id`、`city_code`、`user_id`、`pv` の4つのカラムで構成されており、`site_id` と `city_code` がソートキーカラムとして選択されています。

テーブルを作成するためのステートメントは次のとおりです：

```SQL
CREATE TABLE site_access_duplicate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

### Aggregate Key

`site_access_aggregate` という名前のテーブルを作成します。このテーブルは、`site_id`、`city_code`、`user_id`、`pv` の4つのカラムで構成されており、`site_id` と `city_code` がソートキーカラムとして選択されています。

テーブルを作成するためのステートメントは次のとおりです：

```SQL
CREATE TABLE site_access_aggregate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id BITMAP BITMAP_UNION,
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

>**NOTICE**
>
> 集計テーブルでは、`agg_type` が指定されていないカラムはキーのカラムであり、`agg_type` が指定されているカラムは値のカラムです。詳細は [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) を参照してください。前述の例では、`site_id` と `city_code` のみがソートキーカラムとして指定されているため、`user_id` と `pv` には `agg_type` を指定する必要があります。

### Unique Key

`site_access_unique` という名前のテーブルを作成します。このテーブルは、`site_id`、`city_code`、`user_id`、`pv` の4つのカラムで構成されており、`site_id` と `city_code` がソートキーカラムとして選択されています。

テーブルを作成するためのステートメントは次のとおりです：

```SQL
CREATE TABLE site_access_unique
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
UNIQUE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

### Primary Key

`site_access_primary` という名前のテーブルを作成します。このテーブルは、`site_id`、`city_code`、`user_id`、`pv` の4つのカラムで構成されており、`site_id` と `city_code` がソートキーカラムとして選択されています。

テーブルを作成するためのステートメントは次のとおりです：

```SQL
CREATE TABLE site_access_primary
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
PRIMARY KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id) BUCKETS 10;
```

## ソートの効果

前述のテーブルを例に使用します。ソートの効果は次の3つの状況で異なります：

- クエリが `site_id` と `city_code` の両方でフィルタリングされる場合、StarRocks がクエリ中にスキャンする必要のある行数が大幅に減少します：

  ```Plain
  select sum(pv) from site_access_duplicate where site_id = 123 and city_code = 2;
  ```

- クエリが `site_id` のみでフィルタリングされる場合、StarRocks はクエリ範囲を `site_id` 値を含む行に絞り込むことができます：

  ```Plain
  select sum(pv) from site_access_duplicate where site_id = 123;
  ```

- クエリが `city_code` のみでフィルタリングされる場合、StarRocks はテーブル全体をスキャンする必要があります：

  ```Plain
  select sum(pv) from site_access_duplicate where city_code = 2;
  ```

  > **NOTE**
  >
  > この状況では、ソートキーカラムは期待されるソート効果を発揮しません。

上記のように、クエリが `site_id` と `city_code` の両方でフィルタリングされる場合、StarRocks はテーブルに対してバイナリ検索を実行してクエリ範囲を特定の位置に絞り込みます。テーブルが多数の行で構成されている場合、StarRocks は `site_id` と `city_code` カラムに対してバイナリ検索を実行します。これには、StarRocks がこれら2つのカラムのデータをメモリにロードする必要があり、メモリ消費が増加します。この場合、プレフィックスインデックスを使用してメモリにキャッシュされるデータ量を削減し、クエリを高速化できます。

さらに、多数のソートキーカラムもメモリ消費を増加させます。メモリ消費を削減するために、StarRocks はプレフィックスインデックスの使用に次の制限を課しています：

- ブロックのプレフィックスインデックスエントリは、そのブロックの最初の行のテーブルのソートキーカラムのプレフィックスで構成されていなければなりません。

- プレフィックスインデックスは最大3つのカラムに作成できます。

- プレフィックスインデックスエントリは36バイトを超えてはなりません。

- FLOAT または DOUBLE データ型のカラムにプレフィックスインデックスを作成することはできません。

- プレフィックスインデックスが作成されるすべてのカラムのうち、VARCHAR データ型のカラムは1つだけ許可され、そのカラムはプレフィックスインデックスの終端カラムでなければなりません。

- プレフィックスインデックスの終端カラムが CHAR または VARCHAR データ型の場合、プレフィックスインデックスのエントリは36バイトを超えてはなりません。

## ソートキーカラムの選択方法

このセクションでは、`site_access_duplicate` テーブルを例に、ソートキーカラムを選択する方法を説明します。

- クエリが頻繁にフィルタリングするカラムを特定し、これらのカラムをソートキーカラムとして選択することをお勧めします。

- 複数のソートキーカラムを選択する場合、頻繁にフィルタリングされる識別度の高いカラムを他のカラムよりも前にリストすることをお勧めします。
  
  カラムの識別度が高いとは、そのカラムの値の数が多く、継続的に増加することを意味します。例えば、`site_access_duplicate` テーブルの都市の数は固定されており、テーブルの `city_code` カラムの値の数も固定されています。しかし、`site_id` カラムの値の数は `city_code` カラムの値の数よりもはるかに多く、継続的に増加しています。したがって、`site_id` カラムは `city_code` カラムよりも識別度が高いと言えます。

- 多数のソートキーカラムを選択しないことをお勧めします。多数のソートキーカラムはクエリパフォーマンスを向上させることはできませんが、ソートとデータロードのオーバーヘッドを増加させます。

まとめると、`site_access_duplicate` テーブルのソートキーカラムを選択する際には次の点に注意してください：

- クエリが頻繁に `site_id` と `city_code` の両方でフィルタリングされる場合、`site_id` を開始ソートキーカラムとして選択することをお勧めします。

- クエリが頻繁に `city_code` のみでフィルタリングされ、時折 `site_id` と `city_code` の両方でフィルタリングされる場合、`city_code` を開始ソートキーカラムとして選択することをお勧めします。

- クエリが `site_id` と `city_code` の両方でフィルタリングされる回数が `city_code` のみでフィルタリングされる回数とほぼ同じ場合、最初のカラムが `city_code` であるマテリアライズドビューを作成することをお勧めします。これにより、StarRocks はマテリアライズドビューの `city_code` カラムにソートインデックスを作成します。