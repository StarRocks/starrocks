# SET DEFAULT ROLE

## 功能

设置用户登录时默认激活的角色。该命令从 3.0 版本开始支持。

## 语法

```SQL
-- 设置指定角色为用户的默认角色。
SET DEFAULT ROLE <role_name>[,<role_name>,..] TO <user_identity>;
-- 设置用户拥有的所有角色为默认角色，包括未来赋予给用户的角色。
SET DEFAULT ROLE ALL TO <user_identity>;
-- 不设置任何角色为默认角色，但此时用户仍旧会默认激活 public 角色。
SET DEFAULT ROLE NONE TO <user_identity>; 
```

## 参数说明

`role_name`: 用户拥有的角色名。

`user_identity`: 用户标识。

## 注意事项

普通用户可以设置自己的默认角色，`user_admin` 可以为其他用户设置默认角色。设定时，请确认用户已经拥有对应角色。

可以通过 [SHOW GRANTS](SHOW_GRANTS.md) 查看拥有的角色。

## 示例

查看当前用户拥有的角色。

```SQL
SHOW GRANTS FOR test;
+--------------+---------+----------------------------------------------+
| UserIdentity | Catalog | Grants                                       |
+--------------+---------+----------------------------------------------+
| 'test'@'%'   | NULL    | GRANT 'db_admin', 'user_admin' TO 'test'@'%' |
+--------------+---------+----------------------------------------------+
```

示例一：设置 `db_admin`、`user_admin` 角色为 `test` 用户的默认角色。

```SQL
SET DEFAULT ROLE db_admin TO test;
```

示例二：设置 `test` 用户拥有的所有角色为默认角色，包含未来授予给用户的角色。

```SQL
SET DEFAULT ROLE ALL TO test;
```

示例三：清空 `test` 用户的默认角色。

```SQL
SET DEFAULT ROLE NONE TO test;
```
