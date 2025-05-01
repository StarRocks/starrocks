---
displayed_sidebar: docs
---

# ADD SQLBLACKLIST

## 説明

SQL ブラックリストに正規表現を追加して、特定の SQL パターンを禁止します。SQL ブラックリスト機能が有効になっている場合、StarRocks は実行されるすべての SQL ステートメントをブラックリストの SQL 正規表現と比較します。ブラックリストのいずれかの正規表現に一致する SQL は実行されず、エラーが返されます。これにより、特定の SQL がクラスターのクラッシュや予期しない動作を引き起こすのを防ぎます。

SQL ブラックリストの詳細については、 [Manage SQL Blacklist](../../../../administration/management/resource_management/Blacklist.md) を参照してください。

:::tip

- この操作には SYSTEM レベルの BLACKLIST 権限が必要です。この権限を付与するには、 [GRANT](../../account-management/GRANT.md) の指示に従ってください。
- 現在、StarRocks は SELECT ステートメントを SQL ブラックリストに追加することをサポートしています。

:::

## 構文

```SQL
ADD SQLBLACKLIST "<sql_reg_expr>"
```

## パラメータ

`sql_reg_expr`: 特定の SQL パターンを指定するために使用される正規表現です。SQL ステートメント内の特殊文字と正規表現内の特殊文字を区別するために、SQL ステートメント内の特殊文字にはエスケープ文字 `\` を接頭辞として使用する必要があります。例えば、`(`、`)`、`+` などです。ただし、`(` と `)` は SQL ステートメントでよく使用されるため、StarRocks は SQL ステートメント内の `(` と `)` を直接識別できます。`(` と `)` にはエスケープ文字を使用する必要はありません。

## 例

例 1: `count(\*)` を SQL ブラックリストに追加します。

```Plain
mysql> ADD SQLBLACKLIST "select count(\\*) from .+";
```

例 2: `count(distinct )` を SQL ブラックリストに追加します。

```Plain
mysql> ADD SQLBLACKLIST "select count(distinct .+) from .+";
```

例 3: `order by limit x, y, 1 <= x <=7, 5 <=y <=7` を SQL ブラックリストに追加します。

```Plain
mysql> ADD SQLBLACKLIST "select id_int from test_all_type_select1 
    order by id_int 
    limit [1-7], [5-7]";
```

例 4: 複雑な SQL 正規表現を SQL ブラックリストに追加します。この例は、SQL ステートメントで `*` と `-` のエスケープ文字の使用方法を示しています。

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