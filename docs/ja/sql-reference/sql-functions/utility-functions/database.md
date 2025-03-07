---
displayed_sidebar: docs
---

# database

現在のデータベースの名前を返します。データベースが選択されていない場合は、空の値が返されます。

## Syntax

```Haskell
database()
```

## Parameters

この関数はパラメータを必要としません。

## Return value

現在のデータベースの名前を文字列として返します。

## Examples

```sql
-- Select a destination database.
use db_test

-- Query the name of the current database.
select database();
+------------+
| DATABASE() |
+------------+
| db_test    |
+------------+
```

## See also

[USE](../../sql-statements/Database/USE.md): 目的のデータベースに切り替えます。