---
displayed_sidebar: docs
---

# json_contains

JSON ドキュメントが特定の値またはサブドキュメントを含んでいるかどうかを確認します。ターゲット JSON ドキュメントが候補 JSON 値を含んでいる場合、JSON_CONTAINS 関数は `1` を返します。そうでない場合、JSON_CONTAINS 関数は `0` を返します。

:::tip
すべての JSON 関数と演算子はナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

:::

## Syntax

```Haskell
json_contains(json_target, json_candidate)
```

## Parameters

- `json_target`: ターゲット JSON ドキュメントを表す式。このドキュメントは JSON カラム、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトであることができます。

- `json_candidate`: ターゲット内で検索する候補 JSON 値またはサブドキュメントを表す式。この値は JSON カラム、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトであることができます。

## Return value

BOOLEAN 値を返します。

## 使用上の注意

- スカラー値（文字列、数値、ブール値、null）の場合、値が等しい場合に関数は true を返します。
- JSON オブジェクトの場合、ターゲットオブジェクトが候補オブジェクトのすべてのキー・値ペアを含んでいる場合に関数は true を返します。
- JSON 配列の場合、ターゲット配列が候補配列のすべての要素を含んでいるか、候補がターゲット配列に含まれる単一の値である場合に関数は true を返します。
- この関数はネストした構造に対して深い包含チェックを実行します。

## Examples

Example 1: JSON オブジェクトが特定のキー・値ペアを含んでいるかどうかを確認します。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('{"a": 1}'));

       -> 1
```

Example 2: JSON オブジェクトが存在しないキー・値ペアを含んでいるかどうかを確認します。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"a": 1, "b": 2}'), PARSE_JSON('{"c": 3}'));

       -> 0
```

Example 3: JSON 配列が特定の要素を含んでいるかどうかを確認します。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('[2, 3]'));

       -> 1
```

Example 4: JSON 配列が単一のスカラー値を含んでいるかどうかを確認します。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('2'));

       -> 1
```

Example 5: JSON 配列が存在しない要素を含んでいるかどうかを確認します。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('[1, 2, 3, 4]'), PARSE_JSON('[5, 6]'));

       -> 0
```

Example 6: ネストした JSON 構造での包含関係を確認します。

```plaintext
mysql> SELECT json_contains(PARSE_JSON('{"users": [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]}'), 
                           PARSE_JSON('{"users": [{"id": 1}]}'));

       -> 0
```

注意：最後の例では、結果は 0 です。これは、配列の包含には正確な要素マッチングが必要で、配列内のオブジェクトの部分マッチングは行われないためです。