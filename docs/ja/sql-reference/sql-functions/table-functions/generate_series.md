---
displayed_sidebar: docs
---

# generate_series

`start` と `end` で指定された範囲内で、オプションの `step` を使って一連の値を生成します。

generate_series() はテーブル関数です。テーブル関数は、各入力行に対して行セットを返すことができます。行セットは、0 行、1 行、または複数行を含むことができます。各行は、1 列以上を含むことができます。

StarRocks で generate_series() を使用するには、入力パラメータが定数の場合、TABLE キーワードで囲む必要があります。入力パラメータが列名などの式の場合、TABLE キーワードは不要です（例 5 を参照）。

この関数は v3.1 からサポートされています。

## 構文

```SQL
generate_series(start, end [,step])
```

## パラメータ

- `start`: シリーズの開始値（必須）。サポートされているデータ型は INT、BIGINT、LARGEINT です。
- `end`: シリーズの終了値（必須）。サポートされているデータ型は INT、BIGINT、LARGEINT です。
- `step`: 増分または減分の値（オプション）。サポートされているデータ型は INT、BIGINT、LARGEINT です。指定されていない場合、デフォルトのステップは 1 です。`step` は負または正のいずれかである必要がありますが、0 にはできません。

3 つのパラメータは同じデータ型でなければなりません。例えば、`generate_series(INT start, INT end [, INT step])` のようにします。

v3.3 から名前付き引数をサポートしており、すべてのパラメータは `name=>expr` のように名前で入力されます。例えば、`generate_series(start=>3, end=>7, step=>2)` のようにします。名前付き引数は引数の順序を変更でき、デフォルト引数をオプションで設定できますが、位置引数と混在させることはできません。

## 戻り値

入力パラメータ `start` と `end` と同じ一連の値を返します。

- `step` が正の場合、`start` が `end` より大きいと 0 行が返されます。逆に、`step` が負の場合、`start` が `end` より小さいと 0 行が返されます。
- `step` が 0 の場合、エラーが返されます。
- この関数は null を次のように処理します。入力パラメータがリテラル null の場合、エラーが報告されます。入力パラメータが式で、その結果が null の場合、0 行が返されます（例 5 を参照）。

## 例

例 1: デフォルトのステップ `1` で範囲 [2,5] 内の値のシーケンスを昇順で生成します。

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

例 2: 指定されたステップ `2` で範囲 [2,5] 内の値のシーケンスを昇順で生成します。

```SQL
MySQL > select * from TABLE(generate_series(2, 5, 2));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               4 |
+-----------------+
```

例 3: 指定されたステップ `-1` で範囲 [5,2] 内の値のシーケンスを降順で生成します。

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

例 4: `step` が負で `start` が `end` より小さい場合、0 行が返されます。

```SQL
MySQL > select * from TABLE(generate_series(2, 5, -1));
Empty set (0.01 sec)
```

例 5: テーブルの列を generate_series() の入力パラメータとして使用します。この使用例では、generate_series() に `TABLE()` を使用する必要はありません。

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

-- ステップ 1 で行 (1,3) および (4,7) に対して複数の行を生成します。
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

-- ステップ -1 で行 (5,2) および (9,6) に対して複数の行を生成します。
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

例 6: 名前付き引数を使用して、指定されたステップ `2` で範囲 [2,5] 内の値のシーケンスを昇順で生成します。

```SQL
MySQL > select * from TABLE(generate_series(start=>2, end=>5, step=>2));
+-----------------+
| generate_series |
+-----------------+
|               2 |
|               4 |
+-----------------+
```

## キーワード

table function, generate series