---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "DROP USER drops a specified user identity."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# DROP USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.mdx'

DROP USER drops a specified user identity.

<UserManagementPriv />

## Syntax

```sql
 DROP USER '<user_identity>'

`user_identity`:

 user@'host'
user@['domain']
```

## Examples

Drop user `jack@'192.%'`.

```sql
DROP USER 'jack'@'192.%'
```
