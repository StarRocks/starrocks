---
displayed_sidebar: "Chinese"
---

# DROP FUNCTION

## 功能

删除一个自定义函数。函数的名称、参数类型完全一致才能够被删除。

要执行该命令的用户必须是函数的拥有者。

## 语法

```sql
DROP FUNCTION <function_name>(arg_type [, ...])
```

参数说明：

`function_name`: 待删除函数的名字，必填。

`arg_type`: 待删除函数的参数类型，必填。

## 示例

删除一个函数。

```sql
DROP FUNCTION my_add(INT, INT)
```
