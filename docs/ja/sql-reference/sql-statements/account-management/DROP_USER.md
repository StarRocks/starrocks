---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "DROP USER removes a specified user identity from StarRocks."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# DROP USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.mdx'

## 説明

指定されたユーザーアイデンティティを削除します。

<UserManagementPriv />

## 構文

```sql
 DROP USER '<user_identity>'

`user_identity`:

 user@'host'
user@['domain']
```

## 例

ユーザー `jack@'192.%'` を削除します。

```sql
DROP USER 'jack'@'192.%'
```