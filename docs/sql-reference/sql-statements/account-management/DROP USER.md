# DROP USER

## Description

Deletes a specified user identity.

## Syntax

```sql
 DROP USER '<user_identity>'

`user_identity`:

 user@'host'
user@['domain']
```

## Examples

Delete user jack@'192.%'.

```sql
DROP USER 'jack'@'192.%'
```
