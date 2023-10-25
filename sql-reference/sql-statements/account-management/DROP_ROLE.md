# DROP ROLE

## 功能

该语句用户删除一个角色。

## 语法

```sql
DROP ROLE role1;
```

 删除一个角色，不会影响之前属于该角色的用户的权限。仅相当于将该角色与用户解耦。用户已经从该角色中获取到的权限不会改变。

## 示例

1. 删除一个角色

  ```sql
  DROP ROLE role1;
  ```

## 关键字(keywords)

   DROP, ROLE
