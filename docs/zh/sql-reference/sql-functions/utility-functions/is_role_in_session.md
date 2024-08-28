---
displayed_sidebar: docs
---

# is_role_in_session

## 功能

用于检查指定的角色（包括嵌套角色）在当前会话下是否已经激活。

该函数从 3.1.4 版本开始支持。

## 语法

```Haskell
BOOLEAN is_role_in_session(VARCHAR role_name);
```

## 参数说明

`role_name`: 要检查的角色，可以是嵌套角色。支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 BOOLEAN。`0` 表示未激活；`1` 表示角色已激活。

## 示例

1. 创建角色和用户。

   ```sql
   -- 创建三个角色。
   create role r1;
   create role r2;
   create role r3;

   -- 创建用户 u1。
   create user u1;

   -- 将角色一层层传递给角色 r1。然后将角色 r1 赋予给用户 u1，这样用户就拥有了r1, r2, r3 三个角色。
   grant r3 to role r2;
   grant r2 to role r1;
   grant r1 to user u1;

   -- 切换到用户 u1，以 u1 身份执行操作。
   execute as u1 with no revert;
   ```

2. 检查角色 `r1` 是否被激活。结果显示未激活。

   ```plaintext
   select is_role_in_session("r1");
   +--------------------------+
   | is_role_in_session('r1') |
   +--------------------------+
   |                        0 |
   +--------------------------+
   ```

3. 使用 [SET ROLE](../../sql-statements/account-management/SET_ROLE.md) 命令激活角色 `r1`，然后再做检查。结果显示角色 `r1` 已经被激活，`r1` 的两个嵌套角色 `r2` 和 `r3` 也同时被激活。

   ```sql
   set role "r1";

   select is_role_in_session("r1");
   +--------------------------+
   | is_role_in_session('r1') |
   +--------------------------+
   |                        1 |
   +--------------------------+

   select is_role_in_session("r2");
   +--------------------------+
   | is_role_in_session('r2') |
   +--------------------------+
   |                        1 |
   +--------------------------+

   select is_role_in_session("r3");
   +--------------------------+
   | is_role_in_session('r3') |
   +--------------------------+
   |                        1 |
   +--------------------------+
   ```
