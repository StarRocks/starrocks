---
displayed_sidebar: docs
---

# SHOW GRANTS

## 説明

ユーザーまたはロールに付与されたすべての権限を表示します。

ロールと権限の詳細については、 [Overview of privileges](../../../administration/user_privs/privilege_overview.md) を参照してください。

:::tip
すべてのロールとユーザーは、自分に付与された権限や割り当てられたロールを確認できます。`user_admin` ロールを持つユーザーのみが、指定されたユーザーまたはロールの権限を確認できます。
:::

## 構文

```SQL
SHOW GRANTS; -- 現在のユーザーの権限を表示します。
SHOW GRANTS FOR ROLE <role_name>; -- 特定のロールの権限を表示します。
SHOW GRANTS FOR <user_identity>; -- 特定のユーザーの権限を表示します。
```

## パラメータ

- role_name
- user_identity

返されるフィールド:

```SQL
-- 特定のユーザーの権限を表示します。
+--------------+--------+---------------------------------------------+
|UserIdentity  |Catalog | Grants                                      |
+--------------+--------+---------------------------------------------+

-- 特定のロールの権限を表示します。
+-------------+--------+-------------------------------------------------------+
|RoleName     |Catalog | Grants                                                |
+-------------+-----------------+----------------------------------------------+
```

| **フィールド** | **説明**                                                      |
| -------------- | ------------------------------------------------------------- |
| UserIdentity   | ユーザーの権限を照会するときに表示されるユーザーの識別情報。 |
| RoleName       | ロールの権限を照会するときに表示されるロール名。             |
| Catalog        | カタログ名。<br />GRANT 操作が StarRocks 内部カタログで実行された場合は `default` が返されます。<br />GRANT 操作が external catalog で実行された場合は、その external catalog の名前が返されます。<br />`Grants` 列に表示される操作がロールの割り当てである場合は `NULL` が返されます。 |
| Grants         | 特定の GRANT 操作。                                           |

## 例

```SQL
mysql> SHOW GRANTS;
+--------------+---------+----------------------------------------+
| UserIdentity | Catalog | Grants                                 |
+--------------+---------+----------------------------------------+
| 'root'@'%'   | NULL    | GRANT 'root', 'testrole' TO 'root'@'%' |
+--------------+---------+----------------------------------------+

mysql> SHOW GRANTS FOR 'user_g'@'%';
+-------------+-------------+-----------------------------------------------------------------------------------------------+
|UserIdentity |Catalog      |Grants                                                                                         |
+-------------+-------------------------------------------------------------------------------------------------------------+
|'user_g'@'%' |NULL         |GRANT role_g, public to `user_g`@`%`;                                                          | 
|'user_g'@'%' |NULL         |GRANT IMPERSONATE ON USER `user_a`@`%` TO USER `user_g`@`%`;                                |    
|'user_g'@'%' |default      |GRANT CREATE_DATABASE ON CATALOG default_catalog TO USER `user_g`@`%`;                         | 
|'user_g'@'%' |default      |GRANT ALTER, DROP, CREATE_TABLE ON DATABASE db1 TO USER `user_g`@`%`;                          | 
|'user_g'@'%' |default      |GRANT CREATE_VIEW ON DATABASE db1 TO USER `user_g`@`%` WITH GRANT OPTION;                      | 
|'user_g'@'%' |default      |GRANT ALTER, DROP, SELECT, INGEST, EXPORT, DELETE, UPDATE ON TABLE db.* TO USER `user_g`@`%`;  | 
|'user_g'@'%' |default      |GRANT ALTER, DROP, SELECT ON VIEW db2.view TO USER `user_g`@`%`;                               | 
|'user_g'@'%' |Hive_catalog |GRANT USAGE ON CATALOG Hive_catalog TO USER `user_g`@`%`                                       |
+-------------+--------------+-----------------------------------------------------------------------------------------------+

mysql> SHOW GRANTS FOR ROLE role_g;
+-------------+--------+-------------------------------------------------------+
|RoleName     |Catalog | Grants                                                |
+-------------+-----------------+----------------------------------------------+
|role_g       |NULL    | GRANT role_p, role_test TO ROLE role_g;               | 
|role_g       |default | GRANT SELECT ON *.* TO ROLE role_g WITH GRANT OPTION; | 
+-------------+--------+--------------------------------------------------------+
```

## 参考

[GRANT](GRANT.md)