---
displayed_sidebar: docs
---

# array_generate

`start` と `end` で指定された範囲内の異なる値の配列を `step` の増分で返します。

この関数は v3.1 からサポートされています。

## 構文

```Haskell
ARRAY array_generate([start,] end [, step])
```

## パラメータ

- `start`: 任意。開始値です。TINYINT、SMALLINT、INT、BIGINT、または LARGEINT に評価される定数または列でなければなりません。デフォルト値は 1 です。
- `end`: 必須。終了値です。TINYINT、SMALLINT、INT、BIGINT、または LARGEINT に評価される定数または列でなければなりません。
- `step`: 任意。増分です。TINYINT、SMALLINT、INT、BIGINT、または LARGEINT に評価される定数または列でなければなりません。`start` が `end` より小さい場合、デフォルト値は 1 です。`start` が `end` より大きい場合、デフォルト値は -1 です。

## 戻り値

入力パラメータと同じデータ型の要素を持つ配列を返します。

## 使用上の注意

- 入力パラメータが列の場合、その列が属するテーブルを指定する必要があります。
- 入力パラメータが列の場合、他のパラメータを指定する必要があります。デフォルト値はサポートされていません。
- 入力パラメータが NULL の場合、NULL が返されます。
- `step` が 0 の場合、空の配列が返されます。
- `start` が `end` と等しい場合、その値が返されます。

## 例

### 入力パラメータが定数の場合

```Plain Text
mysql> select array_generate(9);
+---------------------+
| array_generate(9)   |
+---------------------+
| [1,2,3,4,5,6,7,8,9] |
+---------------------+

select array_generate(9,12);
+-----------------------+
| array_generate(9, 12) |
+-----------------------+
| [9,10,11,12]          |
+-----------------------+

select array_generate(9,6);
+----------------------+
| array_generate(9, 6) |
+----------------------+
| [9,8,7,6]            |
+----------------------+

select array_generate(9,6,-1);
+--------------------------+
| array_generate(9, 6, -1) |
+--------------------------+
| [9,8,7,6]                |
+--------------------------+

select array_generate(3,3);
+----------------------+
| array_generate(3, 3) |
+----------------------+
| [3]                  |
+----------------------+
```

### 入力パラメータの一つが列の場合

```sql
CREATE TABLE `array_generate`
(
  `c1` TINYINT,
  `c2` SMALLINT,
  `c3` INT
)
ENGINE = OLAP
DUPLICATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`);

INSERT INTO `array_generate` VALUES
(1, 6, 3),
(2, 9, 4);
```

```Plain Text
mysql> select array_generate(1,c2,2) from `array_generate`;
+--------------------------+
| array_generate(1, c2, 2) |
+--------------------------+
| [1,3,5]                  |
| [1,3,5,7,9]              |
+--------------------------+
```