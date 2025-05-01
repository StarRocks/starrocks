---
displayed_sidebar: docs
---

# DROP USER

## 説明

指定されたユーザーアイデンティティを削除します。

## 構文

```sql
 DROP USER '<user_identity>'

`user_identity`:

 user@'host'
user@['domain']
```

## 例

ユーザー jack@'192.%' を削除します。

```sql
DROP USER 'jack'@'192.%'
```