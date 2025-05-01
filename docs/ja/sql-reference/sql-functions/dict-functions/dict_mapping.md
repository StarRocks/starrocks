---
displayed_sidebar: docs
---

# dict_mapping

## 説明

辞書テーブルで指定されたキーにマッピングされた値を返します。

この関数は主にグローバル辞書テーブルの適用を簡素化するために使用されます。ターゲットテーブルへのデータロード中に、StarRocks はこの関数の入力パラメータを使用して辞書テーブルから指定されたキーにマッピングされた値を自動的に取得し、その値をターゲットテーブルにロードします。

バージョン v3.2.5 以降、StarRocks はこの関数をサポートしています。また、現在 StarRocks の共有データモードではこの関数はサポートされていないことに注意してください。

## 構文

```SQL
dict_mapping("[<db_name>.]<dict_table>", key_column_expr_list [, <value_column> ] [, <null_if_not_exist>] )

key_column_expr_list ::= key_column_expr [, key_column_expr ... ]

key_column_expr ::= <column_name> | <expr>
```

## パラメータ

- 必須パラメータ:
  - `[<db_name>.]<dict_table>`: 辞書テーブルの名前で、主キーテーブルである必要があります。サポートされるデータ型は VARCHAR です。
  - `key_column_expr_list`: 辞書テーブルのキー列の式リストで、1つまたは複数の `key_column_exprs` を含みます。`key_column_expr` は辞書テーブルのキー列の名前、または特定のキーやキー式であることができます。

    この式リストには辞書テーブルのすべての主キー列を含める必要があります。つまり、式の総数は辞書テーブルの主キー列の総数と一致する必要があります。そのため、辞書テーブルが複合主キーを使用している場合、このリストの式はテーブルスキーマで定義された主キー列に順番に対応する必要があります。このリスト内の複数の式はカンマ（`,`）で区切られます。また、`key_column_expr` が特定のキーやキー式である場合、その型は辞書テーブルの対応する主キー列の型と一致する必要があります。

- オプションパラメータ:
  - `<value_column>`: 値の列の名前で、マッピング列でもあります。値の列が指定されていない場合、デフォルトの値の列は辞書テーブルの AUTO_INCREMENT 列です。値の列は、辞書テーブル内の自動インクリメント列および主キーを除く任意の列として定義できます。列のデータ型には制限がありません。
  - `<null_if_not_exist>` (オプション): キーが辞書テーブルに存在しない場合に返すかどうか。 有効な値:
    - `true`: キーが存在しない場合、NULL が返されます。
    - `false` (デフォルト): キーが存在しない場合、例外がスローされます。

## 戻り値

返される値のデータ型は、値の列のデータ型と一致します。値の列が辞書テーブルの自動インクリメント列である場合、返される値のデータ型は BIGINT です。

ただし、指定されたキーにマッピングされた値が見つからない場合、`<null_if_not_exist>` パラメータが `true` に設定されていると `NULL` が返されます。パラメータが `false`（デフォルト）に設定されている場合、エラー `query failed if record not exist in dict table` が返されます。

## 例

**例 1: 辞書テーブルからキーにマッピングされた値を直接クエリします。**

1. 辞書テーブルを作成し、シミュレートされたデータをロードします。

      ```SQL
      MySQL [test]> CREATE TABLE dict (
          order_uuid STRING,
          order_id_int BIGINT AUTO_INCREMENT 
      )
      PRIMARY KEY (order_uuid)
      DISTRIBUTED BY HASH (order_uuid);
      Query OK, 0 rows affected (0.02 sec)
      
      MySQL [test]> INSERT INTO dict (order_uuid) VALUES ('a1'), ('a2'), ('a3');
      Query OK, 3 rows affected (0.12 sec)
      {'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
      
      MySQL [test]> SELECT * FROM dict;
      +------------+--------------+
      | order_uuid | order_id_int |
      +------------+--------------+
      | a1         |            1 |
      | a3         |            3 |
      | a2         |            2 |
      +------------+--------------+
      3 rows in set (0.01 sec)
      ```

      > **注意**
      >
      > 現在、`INSERT INTO` 文は部分更新をサポートしていません。そのため、`dict` のキー列に挿入される値が重複しないようにしてください。そうしないと、辞書テーブルに同じキー列の値を複数回挿入すると、値の列にマッピングされた値が変更されます。

2. 辞書テーブルでキー `a1` にマッピングされた値をクエリします。

    ```SQL
    MySQL [test]> SELECT dict_mapping('dict', 'a1');
    +----------------------------+
    | dict_mapping('dict', 'a1') |
    +----------------------------+
    |                          1 |
    +----------------------------+
    1 row in set (0.01 sec)
    ```

**例 2: テーブル内のマッピング列が `dict_mapping` 関数を使用して生成列として構成されている場合。StarRocks はこのテーブルにデータをロードする際にキーにマッピングされた値を自動的に取得できます。**

1. データテーブルを作成し、`dict_mapping('dict', order_uuid)` を使用してマッピング列を生成列として構成します。

    ```SQL
    CREATE TABLE dest_table1 (
        id BIGINT,
        -- この列は STRING 型の注文番号を記録し、例 1 の dict テーブルの order_uuid 列に対応します。
        order_uuid STRING, 
        batch int comment '異なるバッチロードを区別するために使用されます',
        -- この列は BIGINT 型の注文番号を記録し、order_uuid 列とマッピングされています。
        -- この列は dict_mapping で構成された生成列であるため、この列の値はデータロード中に例 1 の dict テーブルから自動的に取得されます。
        -- その後、この列は重複排除や JOIN クエリに直接使用できます。
        order_id_int BIGINT AS dict_mapping('dict', order_uuid)
    )
    DUPLICATE KEY (id, order_uuid)
    DISTRIBUTED BY HASH(id);
    ```

2. `order_id_int` 列が `dict_mapping('dict', 'order_uuid')` として構成されているこのテーブルにシミュレートされたデータをロードすると、StarRocks は `dict` テーブル内のキーと値のマッピング関係に基づいて `order_id_int` 列に値を自動的にロードします。

      ```SQL
      MySQL [test]> INSERT INTO dest_table1(id, order_uuid, batch) VALUES (1, 'a1', 1), (2, 'a1', 1), (3, 'a3', 1), (4, 'a3', 1);
      Query OK, 4 rows affected (0.05 sec) 
      {'label':'insert_e191b9e4-8a98-11ee-b29c-00163e03897d', 'status':'VISIBLE', 'txnId':'72'}
      
      MySQL [test]> SELECT * FROM dest_table1;
      +------+------------+-------+--------------+
      | id   | order_uuid | batch | order_id_int |
      +------+------------+-------+--------------+
      |    1 | a1         |     1 |            1 |
      |    4 | a3         |     1 |            3 |
      |    2 | a1         |     1 |            1 |
      |    3 | a3         |     1 |            3 |
      +------+------------+-------+--------------+
      4 rows in set (0.02 sec)
      ```

    この例での `dict_mapping` の使用は、[重複排除計算と JOIN クエリ](../../../using_starrocks/query_acceleration_with_auto_increment.md) を加速できます。グローバル辞書を構築して正確な重複排除を加速する以前のソリューションと比較して、`dict_mapping` を使用したソリューションはより柔軟でユーザーフレンドリーです。マッピング値は、キーと値の間のマッピング関係をテーブルにロードする段階で辞書テーブルから直接取得されます。マッピング値を取得するために辞書テーブルをジョインするステートメントを書く必要はありません。さらに、このソリューションはさまざまなデータロード方法をサポートしています。

**例 3: テーブル内のマッピング列が生成列として構成されていない場合、データをテーブルにロードする際にマッピング列に対して `dict_mapping` 関数を明示的に構成し、キーにマッピングされた値を取得する必要があります。**

> **注意**
>
> 例 3 と例 2 の違いは、データテーブルにインポートする際に、インポートコマンドを変更してマッピング列に対して `dict_mapping` 式を明示的に構成する必要があることです。

1. テーブルを作成します。

    ```SQL
    CREATE TABLE dest_table2 (
        id BIGINT,
        order_uuid STRING,
        order_id_int BIGINT NULL,
        batch int comment '異なるバッチロードを区別するために使用されます'
    )
    DUPLICATE KEY (id, order_uuid, order_id_int)
    DISTRIBUTED BY HASH(id);
    ```

2. このテーブルにシミュレートされたデータをロードする際に、`dict_mapping` を構成して辞書テーブルからマッピングされた値を取得します。

    ```SQL
    MySQL [test]> INSERT INTO dest_table2 VALUES (1, 'a1', dict_mapping('dict', 'a1'), 1);
    Query OK, 1 row affected (0.35 sec)
    {'label':'insert_19872ab6-8a96-11ee-b29c-00163e03897d', 'status':'VISIBLE', 'txnId':'42'}

    MySQL [test]> SELECT * FROM dest_table2;
    +------+------------+--------------+-------+
    | id   | order_uuid | order_id_int | batch |
    +------+------------+--------------+-------+
    |    1 | a1         |            1 |     1 |
    +------+------------+--------------+-------+
    1 row in set (0.02 sec)
    ```

**例 4: null_if_not_exist モードを有効にする**

`<null_if_not_exist>` モードが無効で、辞書テーブルに存在しないキーにマッピングされた値がクエリされた場合、`NULL` ではなくエラーが返されます。これにより、データ行のキーが最初に辞書テーブルにロードされ、そのマッピングされた値（辞書 ID）が生成されてから、そのデータ行がターゲットテーブルにロードされることが保証されます。

```SQL
MySQL [test]>  SELECT dict_mapping('dict', 'b1', true);
ERROR 1064 (HY000): Query failed if record not exist in dict table.
```

**例 5: 辞書テーブルが複合主キーを使用している場合、クエリ時にすべての主キーを指定する必要があります。**

1. 複合主キーを持つ辞書テーブルを作成し、シミュレートされたデータをロードします。

      ```SQL
      MySQL [test]> CREATE TABLE dict2 (
          order_uuid STRING,
          order_date DATE, 
          order_id_int BIGINT AUTO_INCREMENT
      )
      PRIMARY KEY (order_uuid,order_date)  -- 複合主キー
      DISTRIBUTED BY HASH (order_uuid,order_date)
      ;
      Query OK, 0 rows affected (0.02 sec)
      
      MySQL [test]> INSERT INTO dict2 VALUES ('a1','2023-11-22',default), ('a2','2023-11-22',default), ('a3','2023-11-22',default);
      Query OK, 3 rows affected (0.12 sec)
      {'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
      
      
      MySQL [test]> select * from dict2;
      +------------+------------+--------------+
      | order_uuid | order_date | order_id_int |
      +------------+------------+--------------+
      | a1         | 2023-11-22 |            1 |
      | a3         | 2023-11-22 |            3 |
      | a2         | 2023-11-22 |            2 |
      +------------+------------+--------------+
      3 rows in set (0.01 sec)
      ```

2. 辞書テーブルでキーにマッピングされた値をクエリします。辞書テーブルが複合主キーを持っているため、`dict_mapping` で全ての主キーを指定する必要があります。

      ```SQL
      SELECT dict_mapping('dict2', 'a1', cast('2023-11-22' as DATE));
      ```

   主キーを1つだけ指定するとエラーが発生します。

      ```SQL
      MySQL [test]> SELECT dict_mapping('dict2', 'a1');
      ERROR 1064 (HY000): Getting analyzing error. Detail message: dict_mapping function param size should be 3 - 5.
      ```