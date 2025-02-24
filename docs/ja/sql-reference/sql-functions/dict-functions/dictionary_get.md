---
displayed_sidebar: docs
---

# dictionary_get

辞書オブジェクト内のキーにマップされた値をクエリします。

## 構文

```SQL
dictionary_get('dictionary_object_name', key_expression_list, [NULL_IF_NOT_EXIST])

key_expression_list ::=
    key_expression [, ...]

key_expression ::=
    column_name | const_value
```

## パラメーター

- `dictionary_name`: 辞書オブジェクトの名前。
- `key_expression_list`: すべてのキー列の式のリスト。列名のリストまたは値のリストにすることができます。
- `NULL_IF_NOT_EXIST` (オプション): 辞書キャッシュにキーが存在しない場合に返すかどうか。有効な値:
  - `true`: キーが存在しない場合、Null が返されます。
  - `false` (デフォルト): キーが存在しない場合、例外がスローされます。

## 戻り値

値列の値を STRUCT 型として返します。そのため、`[N]` または `.<column_name>` を使用して特定の列の値を指定できます。`N` は列の位置を表し、1 から始まります。

## 例

以下の例は、[dict_mapping](dict_mapping.md) の例のデータセットを使用しています。

- 例 1: 辞書オブジェクト `dict_obj` のキー列 `order_uuid` にマップされた値列の値をクエリします。

    ```Plain
    MySQL > SELECT dictionary_get('dict_obj', order_uuid) FROM dict;
    +--------------------+
    | DICTIONARY_GET     |
    +--------------------+
    | {"order_id_int":1} |
    | {"order_id_int":3} |
    | {"order_id_int":2} |
    +--------------------+
    3 rows in set (0.02 sec)
    ```

- 例 2: 辞書オブジェクト `dict_obj` のキー `a1` にマップされた値列の値をクエリします。

    ```Plain
    MySQL > SELECT dictionary_get("dict_obj", "a1");
    +--------------------+
    | DICTIONARY_GET     |
    +--------------------+
    | {"order_id_int":1} |
    +--------------------+
    1 row in set (0.01 sec)
    ```

- 例 3: 辞書オブジェクト `dimension_obj` のキー `1` にマップされた値列の値をクエリします。

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1);
    +-----------------------------------------------------------------------------------------------------------------+
    | DICTIONARY_GET                                                                                                  |
    +-----------------------------------------------------------------------------------------------------------------+
    | {"ProductName":"T-Shirt","Category":"Apparel","SubCategory":"Shirts","Brand":"BrandA","Color":"Red","Size":"M"} |
    +-----------------------------------------------------------------------------------------------------------------+
    1 row in set (0.01 sec)
    ```

- 例 4: 辞書オブジェクト `dimension_obj` のキー `1` にマップされた最初の値列の値をクエリします。

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1)[1];
    +-------------------+
    | DICTIONARY_GET[1] |
    +-------------------+
    | T-Shirt           |
    +-------------------+
    1 row in set (0.01 sec)
    ```

- 例 5: 辞書オブジェクト `dimension_obj` のキー `1` にマップされた2番目の値列の値をクエリします。

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1)[2];
    +-------------------+
    | DICTIONARY_GET[2] |
    +-------------------+
    | Apparel           |
    +-------------------+
    1 row in set (0.01 sec)
    ```

- 例 6: 辞書オブジェクト `dimension_obj` のキー `1` にマップされた `ProductName` 値列の値をクエリします。

    ```Plain
    MySQL > SELECT dictionary_get("dimension_obj", 1).ProductName;
    +----------------------------+
    | DICTIONARY_GET.ProductName |
    +----------------------------+
    | T-Shirt                    |
    +----------------------------+
    1 row in set (0.01 sec)
    ```