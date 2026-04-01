---
displayed_sidebar: docs
sidebar_label: "Subquery"
---

# サブクエリ

サブクエリは、関連性に関して次の2つのタイプに分類されます。

- 非相関サブクエリ：外側のクエリとは独立して結果を取得します。
- 相関サブクエリ：外側のクエリからの値を必要とします。

## 相関のないサブクエリ

相関のないサブクエリは、[NOT] IN と EXISTS をサポートしています。

**例**

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);

SELECT * FROM t1 WHERE (x,y) [NOT] IN (SELECT x,y FROM t2 LIMIT 2);

SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

v3.0 以降では、`SELECT... FROM... WHERE... [NOT] IN` の WHERE 句で複数のフィールドを指定できます。たとえば、2 番目の SELECT ステートメントの `WHERE (x,y)` などです。

## 相関サブクエリ

相関サブクエリは、[NOT] IN と [NOT] EXISTS をサポートします。

**例**

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

サブクエリは、スカラー量子クエリもサポートしています。これは、無相関スカラー量子クエリ、相関スカラー量子クエリ、および一般関数のパラメータとしてのスカラー量子クエリに分類できます。

## 例

1. 述語が=符号の、無相関スカラー量子クエリ。たとえば、最も高い賃金を持つ人物に関する情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. 相関のないスカラー量子クエリ（述語`>`、`<`などを使用）。例えば、平均より高い給与を得ている人々の情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. 関連するスカラー量子クエリ。たとえば、各部署の最高給与情報を出力します。

    ```sql
    SELECT name FROM table a WHERE salary = (SELECT MAX(salary) FROM table b WHERE b.Department= a.Department);
    ```

4. スカラー量子クエリは、通常の関数のパラメーターとして使用されます。

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```
