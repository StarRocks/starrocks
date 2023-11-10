---
displayed_sidebar: "Chinese"
---

# SET PASSWORD

## 功能

SET PASSWORD 命令可以用于修改一个用户的登录密码。

您也可以使用 [ALTER USER](ALTER_USER.md) 来修改用户密码。

> **注意**
>
> - 任何用户都可以重置自己的密码。
> - 只有 `user_admin` 角色才可以修改其他用户的密码。
> - root 用户的密码仅 root 用户自身可以重置。具体信息，参见 [管理用户权限 - 重置丢失的 root 密码](../../../administration/User_privilege.md#修改用户)。

## 语法

```SQL
SET PASSWORD [FOR user_identity] =
[PASSWORD('plain password')]|['hashed password']
```

如果 `[FOR user_identity]` 字段不存在，那么修改当前用户的密码。

注意此处的 `user_identity` 语法与 [CREATE USER](../account-management/CREATE_USER.md) 章节中的相同。且必须为使用 `CREATE USER` 创建过的 `user_identity`。否则会报错用户不存在。如果不指定 `user_identity`，则当前用户为 `'username'@'ip'`，这个当前用户，可能无法匹配任何 `user_identity`。可以通过 `SHOW GRANTS;` 查看当前用户。

**PASSWORD()** 方式输入的是明文密码; 而直接使用字符串，需要传递的是已加密的密码。

## 示例

示例一： 修改当前用户的密码。

```SQL
SET PASSWORD = PASSWORD('123456');
SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

示例二： 修改指定用户的密码。

```SQL
SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456');
SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```
