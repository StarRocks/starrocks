---
displayed_sidebar: docs
---

# DROP USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.md'

## Description

Drops a specified user identity.

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
