---
displayed_sidebar: docs
---

# SUSPEND or RESUME PIPE

## Description

Suspends or resumes a pipe:

- When a load job is in progress (namely, in the `RUNNING` state), suspending (`SUSPEND`) the pipe for the job interrupts the job.
- When a load job encounters errors, resuming (`RESUME`) the pipe for the job will continue to run the erroneous job.

This command is supported from v3.2 onwards.

## Syntax

```SQL
ALTER PIPE <pipe_name> { SUSPEND | RESUME [ IF SUSPENDED ] }
```

## Parameters

### pipe_name

The name of the pipe.

## Examples

### Suspend a pipe

Suspend the pipe named `user_behavior_replica` (which is in the `RUNNING` state) in the database named `mydatabase`:

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica SUSPEND;
```

If you use [SHOW PIPES](SHOW_PIPES.md) to query the pipe, you can see that its state has changed to `SUSPEND`.

### Resume a pipe

Resume the pipe named `user_behavior_replica` in the database named `mydatabase`:

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica RESUME;
```

If you use [SHOW PIPES](SHOW_PIPES.md) to query the pipe, you can see that its state has changed to `RUNNING`.

## References

- [CREATE PIPE](CREATE_PIPE.md)
- [ALTER PIPE](ALTER_PIPE.md)
- [DROP PIPE](DROP_PIPE.md)
- [SHOW PIPES](SHOW_PIPES.md)
- [RETRY FILE](RETRY_FILE.md)
