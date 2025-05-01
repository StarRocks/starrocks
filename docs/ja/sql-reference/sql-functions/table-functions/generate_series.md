---
displayed_sidebar: docs
---

# generate_series

## Description

`start` と `end` で指定された範囲内で、オプションの `step` を使用して一連の値を生成します。

generate_series() はテーブル関数です。テーブル関数は、各入力行に対して行セットを返すことができます。行セットは、0 行、1 行、または複数の行を含むことができます。各行は、1 つ以上の列を含むことができます。

StarRocks で generate_series() を使用するには、入力パラメータが定数の場合、TABLE キーワードで囲む必要があります。入力パラメータが列名などの式の場合、TABLE キーワードは必要ありません（例 5 を参照）。

この関数は v3.1 からサポートされています。

## Syntax

```SQL
generate_series(start, end [,step])
```

## Parameters

- `start`: シリーズの開始値（必須）。サポートされているデータ型は INT、BIGINT、および LARGEINT です。
- `end`: シリーズの終了値（必須）。サポートされているデータ型は INT、BIGINT、および LARGEINT です。
- `step`: 増分または減分の値（オプション）。サポートされているデータ型は INT、BIGINT、および LARGEINT です。指定されていない場合、デフォルトのステップは 1 です。`step` は負または正のいずれかであることができますが、0 であってはなりません。

3 つのパラメータは同じデータ型でなければなりません。たとえば、`generate_series(INT start, INT end [, INT step])` のようにします。

## Return value

入力パラメータ `start` と `end` と同じ値のシリーズを返します。

- `step` が正の場合、`start` が `end` より大きいと 0 行が返されます。逆に、`step` が負の場合、`start` が `end` より小さいと 0 行が返されます。
- `step` が 0 の場合、エラーが返されます。
- この関数は null を次のように処理します。入力パラメータがリテラル null の場合、エラーが報告されます。入力パラメータが式で、その結果が null の場合、0 行が返されます（例 5 を参照）。

## Examples

Example 1: 範囲 [2,5] 内でデフォルトのステップ `1` で昇順に値のシーケンスを生成します。

```SQL
MySQL > select * from TABLE(generate_series(2, 5));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               3 |
|               4 |
|               5 |
+-----------------+
```

Example 2: 範囲 [2,5] 内で指定されたステップ `2` で昇順に値のシーケンスを生成します。

```SQL
MySQL > select * from TABLE(generate_series(2, 5, 2));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               4 |
+-----------------+
```

Example 3: 範囲 [5,2] 内で指定されたステップ `-1` で降順に値のシーケンスを生成します。

```SQL
MySQL > select * from TABLE(generate_series(5, 2, -1));
+-----------------+
| generate_series |
+-----------------+
|               5 |
|               4 |
|               3 |
|               2 |
+-----------------+
```

Example 4: `step` が負で `start` が `end` より小さい場合、0 行が返されます。

```SQL
MySQL > select * from TABLE(generate_series(2, 5, -1));
Empty set (0.01 sec)
```

Example 5: テーブルの列を generate_series() の入力パラメータとして使用します。この使用例では、generate_series() に `TABLE()` を使用する必要はありません。

```SQL
CREATE TABLE t_numbers(start INT, end INT)
DUPLICATE KEY (start)
DISTRIBUTED BY HASH(start) BUCKETS 1;

INSERT INTO t_numbers VALUES
(1, 3),
(5, 2),
(NULL, 10),
(4, 7),
(9,6);

SELECT * FROM t_numbers;
+-------+------+
| start | end  |
+-------+------+
|  NULL |   10 |
|     1 |    3 |
|     4 |    7 |
|     5 |    2 |
|     9 |    6 |
+-------+------+

-- 行 (1,3) と (4,7) に対してステップ 1 で複数の行を生成します。
SELECT * FROM t_numbers, generate_series(t_numbers.start, t_numbers.end);
+-------+------+-----------------+
| start | end  | generate_series |
+-------+------+-----------------+
|     1 |    3 |               1 |
|     1 |    3 |               2 |
|     1 |    3 |               3 |
|     4 |    7 |               4 |
|     4 |    7 |               5 |
|     4 |    7 |               6 |
|     4 |    7 |               7 |
+-------+------+-----------------+

-- 行 (5,2) と (9,6) に対してステップ -1 で複数の行を生成します。
SELECT * FROM t_numbers, generate_series(t_numbers.start, t_numbers.end, -1);
+-------+------+-----------------+
| start | end  | generate_series |
+-------+------+-----------------+
|     5 |    2 |               5 |
|     5 |    2 |               4 |
|     5 |    2 |               3 |
|     5 |    2 |               2 |
|     9 |    6 |               9 |
|     9 |    6 |               8 |
|     9 |    6 |               7 |
|     9 |    6 |               6 |
+-------+------+-----------------+
```

入力行 `(NULL, 10)` は NULL 値を持ち、この行に対しては 0 行が返されます。

## keywords

table function, generate series