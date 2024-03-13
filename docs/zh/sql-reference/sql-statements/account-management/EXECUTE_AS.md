---
displayed_sidebar: "Chinese"
---

# EXECUTE AS

## 功能

在得到以指定用户身份执行操作的权限 (IMPERSONATE) 后，您可以使用 EXECUTE AS 语句将当前会话的执行上下文切换到该指定用户。该功能从 2.4 版本开始支持。

## 语法

```SQL
EXECUTE AS user WITH NO REVERT
```

## 参数说明

`user`：该指定用户必须为已存在的用户。

## 注意事项

- 当前登录用户（即调用 EXECUTE AS 语句的用户）必须有 IMPERSONATE 到指定用户的权限。
- EXECUTE AS 语句必须包括 WITH NO REVERT 子句，表示在当前会话结束前，当前会话的执行上下文不能切换为原登录用户。

## 示例

将会话的执行上下文切换到用户 `test2`。

```SQL
EXECUTE AS test2 WITH NO REVERT;
```

语句执行成功后，可以通过 `select current_user()` 命令获取当前用户。

```plain
select current_user();
+-----------------------------+
| CURRENT_USER()              |
+-----------------------------+
| 'default_cluster:test2'@'%' |
+-----------------------------+
```
