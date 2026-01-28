---
displayed_sidebar: docs
sidebar_position: 0.95
---

# SQL UDF

バージョン 4.1 以降、StarRocks は SQL UDF の作成をサポートし、ユーザーは SQL 式を使用して関数を作成できるようになりました。

SQL UDF は、SQL 式を再利用可能な関数にカプセル化する軽量な関数定義方法であり、クエリ時に実際の SQL 式に動的に展開されます。

## 機能概要

SQL UDF の主な特徴：

- **動的展開**: クエリ最適化フェーズで関数呼び出しを実際の SQL 式に展開
- **型安全性**: パラメータ型と戻り値型の明示的な宣言をサポート
- **パラメータ化**: 名前付きパラメータをサポートし、関数の可読性を向上

適用シナリオ：
- 再利用が必要な複雑な SQL 式
- 単純なデータ変換と計算ロジック
- 標準化されたデータ処理ルール

## 構文

### SQL UDF の作成

```SQL
CREATE [GLOBAL] FUNCTION
    function_name(arg1_name arg1_type, arg2_name arg2_type, ...)
RETURNS expression
```

#### パラメータ

| **パラメータ**     | **説明**                                                                                             |
| -------------------|------------------------------------------------------------------------------------------------------|
| GLOBAL             | オプション。指定すると、すべてのデータベースで参照可能なグローバル関数を作成                          |
| function_name      | 関数名。データベース名を含めることが可能（例：`db1.my_func`）                                       |
| arg_name           | パラメータ名。式内で参照するために使用                                                               |
| arg_type           | パラメータ型。StarRocks のすべての基本データ型をサポート                                              |
| expression         | SQL 式。関数呼び出し時に実際の式に展開される                                                          |

### SQL UDF の削除

```SQL
DROP FUNCTION [IF EXISTS] function_name(arg_type [, ...])
```

### SQL UDF の参照

```SQL
SHOW [GLOBAL] FUNCTIONS;
```

## 使用例

### 例 1: 文字列処理関数

```SQL
-- 文字列を大文字に変換し、プレフィックスを追加する関数を作成
CREATE FUNCTION format_username(name STRING)
RETURNS concat('USER_', upper(name));

-- 関数を使用
SELECT format_username('alice');
-- 結果: USER_ALICE

-- クエリで使用
SELECT format_username(username) as display_name FROM users;
```

### 例 2: 多パラメータ計算関数

```SQL
-- 割引後の価格を計算する関数を作成
CREATE FUNCTION calculate_discount_price(original_price DECIMAL(10,2), discount_rate DOUBLE)
RETURNS original_price * (1 - discount_rate);

-- 関数を使用
SELECT calculate_discount_price(100.00, 0.2);  -- 結果: 80.00
SELECT calculate_discount_price(price, 0.15) as final_price FROM products;
```

### 例 3: 複雑な式のカプセル化

```SQL
-- JSON を抽出して変換する関数を作成
CREATE FUNCTION extract_user_info(json_str STRING, field_name STRING)
RETURNS get_json_string(get_json_string(json_str, concat('$.', field_name)), '$.value');

-- 複雑なネスト呼び出しを簡略化
SELECT extract_user_info(user_data, 'email') as user_email FROM events;
```

### 例 4: 条件ロジック関数

```SQL
-- 条件ロジックを持つ関数を作成
CREATE FUNCTION classify_temperature(temp DOUBLE)
RETURNS CASE
    WHEN temp >= 30 THEN 'hot'
    WHEN temp >= 20 THEN 'warm'
    WHEN temp >= 10 THEN 'cool'
    ELSE 'cold'
END;

-- 関数を使用
SELECT classify_temperature(25);  -- 結果: warm
```

### 例 5: グローバル SQL UDF

```SQL
-- すべてのデータベースで参照可能なグローバル関数を作成
CREATE GLOBAL FUNCTION format_date_display(dt DATETIME)
RETURNS concat(year(dt), '-', lpad(month(dt), 2, '0'), '-', lpad(day(dt), 2, '0'));

-- どのデータベースでも直接使用可能
SELECT format_date_display(create_time) from my_table;
```

## 高度な機能

### 1. ネスト使用

SQL UDF はネスト呼び出しをサポート：

```SQL
CREATE FUNCTION func_a(x INT, y INT) RETURNS x + y;
CREATE FUNCTION func_b(a INT, b INT) RETURNS func_a(a, b) * 2;

SELECT func_b(3, 4);  -- 結果: 14
```

### 2. 型変換

SQL UDF は暗黙的な型変換をサポート：

```SQL
CREATE FUNCTION convert_and_add(a STRING, b INT)
RETURNS cast(a AS INT) + b;

SELECT convert_and_add('100', 50);  -- 結果: 150
```

### 3. 組み込み関数との組み合わせ

SQL UDF は StarRocks 組み込み関数と自由に組み合わせ可能：

```SQL
CREATE FUNCTION get_year_month(dt DATETIME)
RETURNS concat(year(dt), '-', lpad(month(dt), 2, '0'));

SELECT get_year_month(create_time) as ym FROM events GROUP BY ym;
```

## 表示と管理

### すべての関数の表示

```SQL
-- 現在のデータベースの関数を表示
SHOW FUNCTIONS;

-- すべてのグローバル関数を表示
SHOW GLOBAL FUNCTIONS;
```

### 関数の削除

```SQL
-- 現在のデータベースの関数を削除
DROP FUNCTION format_username(STRING);

-- グローバル関数を削除
DROP GLOBAL FUNCTION format_date_display(DATETIME);

-- IF EXISTS を使用してエラーを回避
DROP FUNCTION IF EXISTS my_function(INT, STRING);
```

### 関数の置換

`OR REPLACE` キーワードを使用して既存の関数を置換：

```SQL
-- 関数定義を変更
CREATE OR REPLACE FUNCTION calculate_tax(amount DECIMAL(10,2))
RETURNS amount * 0.1 + 5.0;
```

## パフォーマンス特性

SQL UDF はクエリ最適化フェーズで実際の SQL 式に展開され、以下のパフォーマンス特性を持つ：

1. **関数呼び出しオーバーヘッドゼロ**: 従来の UDF のような関数呼び出しオーバーヘッドがない
2. **オプティマイザ可視性**: オプティマイザが完全な式を見ることができ、より良い最適化が可能
3. **述語プッシュダウン**: 述語プッシュダウンやその他の最適化ルールをサポート

例えば、以下のクエリ：

```SQL
CREATE FUNCTION my_func(x INT) RETURNS x * 2 + 1;

SELECT * FROM t WHERE my_func(a) > 10;
```

オプティマイザにより以下に展開される：

```SQL
SELECT * FROM t WHERE a * 2 + 1 > 10;
```

## 制限事項と注意点

1. **パラメータ数の制限**: パラメータ数は式で使用される変数と一致する必要がある
2. **型の一致**: パラメータ型は式での使用法と一致する必要がある
