---
displayed_sidebar: docs
---

# DROP PIPE

## Description

Drops a pipe and the related jobs and metadata. Executing this statement on a pipe does not revoke the data that has been loaded via this pipe. This command is supported from v3.2 onwards.

## Syntax

```SQL
DROP PIPE [IF EXISTS] [db_name.]<pipe_name>
```

## Parameters

### db_name

The name of the database to which the pipe belongs.

### pipe_name

The name of the pipe.

## Examples

Drop the pipe named `user_behavior_replica` in the database named `mydatabase`:

```SQL
USE mydatabase;
DROP PIPE user_behavior_replica;
```

## References

- [CREATE PIPE](CREATE_PIPE.md)
- [ALTER PIPE](ALTER_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [SUSPEND or RESUME PIPE](SUSPEND_or_RESUME_PIPE.md)
- [RETRY FILE](RETRY_FILE.md)
