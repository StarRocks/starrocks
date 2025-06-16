---
displayed_sidebar: docs
---

# mann_whitney_u_test

## 説明

2つの母集団から得られたサンプルに対して、Mann-Whitney順位検定を実行します。Mann-Whitney U検定は、2つの母集団が同じ分布から選ばれたかどうかを判断するために使用できるノンパラメトリック検定です。

## 構文

```Haskell
MANN_WHITNEY_U_TEST (sample_data, sample_treatment[, alternative[, continuity_correction]])
```

## パラメータ

- `sample_data`: サンプルデータの値。数値データ型でなければなりません。

- `sample_treatment`: サンプルデータのインデックス。各要素は、対応するサンプルが属する処理グループを示します。値はブール型で、`false`が最初のグループ、`true`が2番目のグループを表します。

- `alternative` (オプション): 対立仮説を指定する定数文字列。以下のいずれかを指定できます:
- - 'two-sided': デフォルト値。2つの母集団の平均が異なるかどうかを検定します。
- - 'less': 最初の母集団の平均が2番目の母集団の平均より小さいかどうかを検定します。
- - 'greater': 最初の母集団の平均が2番目の母集団の平均より大きいかどうかを検定します。

- `continuity_correction` (オプション): 連続性補正を適用するかどうかを示す定数ブール値。連続性補正は、U統計量をU分布の平均に向けて`0.5`調整し、小さなサンプルサイズに対する検定の精度を向上させることができます。デフォルト値は`true`です。

## 戻り値

この関数は、以下の2つの要素を含むjson配列を返します:

Mann-Whitney U統計量と検定に関連するp値。

## 使用上の注意

この関数はNULLを無視します。

## 例

`testing_data`という名前のテーブルがあり、以下のデータが含まれているとします。

```sql
create table testing_data (
    id int, 
    score int, 
    treatment boolean
)
properties(
    "replication_num" = "1"
);

insert into testing_data values 
    (1, 80, false), 
    (2, 100, false), 
    (3, NULL, false), 
    (4, 60, true), 
    (5, 70, true), 
    (6, 85, true);
```

```Plain
select * from testing_data;
+------+-------+-----------+
| id   | score | treatment |
+------+-------+-----------+
|    1 |    80 |         0 |
|    2 |   100 |         0 |
|    3 |  NULL |         0 |
|    4 |    60 |         1 |
|    5 |    70 |         1 |
|    6 |    85 |         1 |
+------+-------+-----------+
```

クエリ:

```SQL
SELECT MANN_WHITNEY_U_TEST(score, treatment) FROM testing_data;
```

結果:

```Plain
+---------------------------------------+
| mann_whitney_u_test(score, treatment) |
+---------------------------------------+
| [5, 0.38647623077123283]              |
+---------------------------------------+
```

クエリ:

```SQL
SELECT MANN_WHITNEY_U_TEST(score, treatment, 'less') FROM testing_data;
```

結果:

```Plain
+-----------------------------------------------+
| mann_whitney_u_test(score, treatment, 'less') |
+-----------------------------------------------+
| [5, 0.9255426634106172]                       |
+-----------------------------------------------+
```

クエリ:

```SQL
SELECT MANN_WHITNEY_U_TEST(score, treatment, 'two-sided', 0) FROM testing_data;
```

結果:

```Plain
+-------------------------------------------------------+
| mann_whitney_u_test(score, treatment, 'two-sided', 0) |
+-------------------------------------------------------+
| [5, 0.2482130789899235]                               |
+-------------------------------------------------------+
```