# CREATE ROLE

## 功能

创建一个角色，对创建好的角色可以进行 [授权](../account-management/GRANT.md#语法) 操作。可参考 [创建用户](../account-management/CREATE_USER.md) 章节最后一个示例对用户进行授权，拥有该角色的用户会拥有角色被赋予的权限。创建好的角色可进行 [删除](../account-management/DROP_ROLE.md).

## 语法

```sql
CREATE ROLE role1;
```

 该语句创建一个无权限的角色，可以后续通过 GRANT 命令赋予该角色权限。

## 示例

 1. 创建一个角色

  ```sql
  CREATE ROLE role1;
  ```

## 关键字(keywords)

CREATE, ROLE
