---
displayed_sidebar: docs
---

# ADD SQLBLACKLIST

## 説明

特定のSQLパターンを禁止するために、正規表現をSQLブラックリストに追加します。SQLブラックリスト機能が有効になっている場合、StarRocksは実行されるすべてのSQL文をブラックリスト内のSQL正規表現と比較します。ブラックリスト内のいずれかの正規表現に一致するSQLは実行されず、エラーが返されます。これにより、特定のSQLがクラスターのクラッシュや予期しない動作を引き起こすのを防ぎます。

SQLブラックリストの詳細については、[Manage SQL Blacklist](../../../../administration/management/resource_management/Blacklist.md)を参照してください。

:::tip

- この操作には、SYSTEMレベルのBLACKLIST権限が必要です。[GRANT](../../account-management/GRANT.md)の指示に従って、この権限を付与できます。
- ブラックリストは、SELECT ステートメント、INSERT ステートメント（v3.1 以降）、および CTAS ステートメント（v3.4 以降）にのみ適用されます。

:::

## 構文

```SQL
ADD SQLBLACKLIST "<sql_reg_expr>"
```

## パラメータ

`sql_reg_expr`: 特定のSQLパターンを指定するために使用される正規表現です。SQL文内の特殊文字と正規表現内の特殊文字を区別するために、SQL文内の特殊文字にはエスケープ文字 `\` を接頭辞として使用する必要があります。例えば、`(`、`)`、`+` などです。ただし、`(` と `)` はSQL文で頻繁に使用されるため、StarRocksはSQL文内の `(` と `)` を直接識別できます。`(` と `)` にはエスケープ文字を使用する必要はありません。

## 例

例1: `count(\*)` をSQLブラックリストに追加します。

```Plain
mysql> ADD SQLBLACKLIST "select count(\\*) from .+";
```

例2: `count(distinct )` をSQLブラックリストに追加します。

```Plain
mysql> ADD SQLBLACKLIST "select count(distinct .+) from .+";
```

例3: `order by limit x, y, 1 <= x <=7, 5 <=y <=7` をSQLブラックリストに追加します。

```Plain
mysql> ADD SQLBLACKLIST "select id_int from test_all_type_select1 
    order by id_int 
    limit [1-7], [5-7]";
```

例4: 複雑なSQL正規表現をSQLブラックリストに追加します。この例は、SQL文内で `*` と `-` にエスケープ文字を使用する方法を示しています。

```Plain
mysql> ADD SQLBLACKLIST 
    "select id_int \\* 4, id_tinyint, id_varchar 
        from test_all_type_nullable 
    except select id_int, id_tinyint, id_varchar 
        from test_basic 
    except select (id_int \\* 9 \\- 8) \\/ 2, id_tinyint, id_varchar 
        from test_all_type_nullable2 
    except select id_int, id_tinyint, id_varchar 
        from test_basic_nullable";
```

例5: すべての INSERT INTO ステートメントを禁止する:

```sql
ADD SQLBLACKLIST "(?i)^insert\\s+into\\s+.*";
```

例6: すべての INSERT INTO ... VALUES ステートメントを禁止する:

```sql
ADD SQLBLACKLIST "(?i)^insert\\s+into\\s+.*values\\s*\\(";
```

例7: システム定義ビュー `_statistics_.column_statistics` に対するものを除き、すべての INSERT INTO ... VALUES ステートメントを禁止する:

```sql
ADD SQLBLACKLIST "(?i)^insert\\s+into\\s+(?!column_statistics\\b).*values\\s*\\(";
```
