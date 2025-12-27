---
displayed_sidebar: docs
---

# is_json_scalar

Returns whether a JSON value is a scalar (not an object or array).

## 別名
なし

## 構文

```Haskell
BOOLEAN is_json_scalar(JSON)
```

### パラメーター

`json` : JSON 型の値。関数は解析済みの JSON 値を調べ、それがスカラーかどうかを判定します。有効な JSON スカラー型には number、string、boolean、および JSON null が含まれます。入力が SQL NULL（または JSON 列の値が NULL）の場合、関数は SQL NULL を返します。

## 戻り値

BOOLEAN を返します:
- TRUE (1) — JSON 値がスカラー（number、string、boolean、または JSON null）の場合。
- FALSE (0) — JSON 値が非スカラー（オブジェクトまたは配列）の場合。
- NULL — 入力が SQL NULL の場合。

## 使用上の注意

- この関数は JSON 型の値に対して動作します。JSON 式を渡すか、文字列リテラルを JSON に CAST してください。例: CAST('{"a": 1}' AS JSON)。
- JSON null（リテラルの JSON 値 null）は、オブジェクトや配列ではないため、この関数ではスカラーと見なされます。
- SQL NULL（値の欠如）は JSON null とは別物です。SQL NULL の入力に対しては NULL が返ります。
- この関数はトップレベルの JSON 値のみを検査します。オブジェクトと配列は、その中身に関係なく非スカラーと見なされます。
- この関数は JsonFunctions::is_json_scalar として実装されており、基になる VPack スライスがオブジェクトでも配列でもないことをチェックします。

## 例

```Plain
mysql> SELECT is_json_scalar(CAST('{"a": 1}' AS JSON));
+----------------------------------------------+
| is_json_scalar(CAST('{"a": 1}' AS JSON))     |
+----------------------------------------------+
| 0                                            |
+----------------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('[1, 2, 3]' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('[1, 2, 3]' AS JSON)) |
+-----------------------------------------+
| 0                                       |
+-----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('"hello"' AS JSON));
+-------------------------------------------+
| is_json_scalar(CAST('"hello"' AS JSON))   |
+-------------------------------------------+
| 1                                         |
+-------------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('123' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('123' AS JSON))    |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('true' AS JSON));
+-----------------------------------------+
| is_json_scalar(CAST('true' AS JSON))    |
+-----------------------------------------+
| 1                                       |
+-----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST('null' AS JSON));
+----------------------------------------+
| is_json_scalar(CAST('null' AS JSON))   |
+----------------------------------------+
| 1                                      |
+----------------------------------------+
```

```Plain
mysql> SELECT is_json_scalar(CAST(NULL AS JSON));
+------------------------------+
| is_json_scalar(CAST(NULL AS JSON)) |
+------------------------------+
| NULL                         |
+------------------------------+
```

## キーワード
IS_JSON_SCALAR, None