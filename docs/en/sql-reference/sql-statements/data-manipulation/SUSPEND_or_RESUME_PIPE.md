# SUSPEND or RESUME PIPE

## Description

Suspends or resumes a pipe:

- When a load job is in progress (namely, in the `RUNNING` state), suspending (`SUSPEND`) the pipe for the job interrupts the job.
- When a load job encounters errors, resuming (`RESUME`) the pipe for the job will continue to run the erroneous job.

## Syntax

```SQL
ALTER PIPE [ IF EXISTS ] [ <pipe_name> ] { SUSPEND | RESUME [ IF SUSPENDED ] }
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

If you use [SHOW PIPES](../../../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md) to query the pipe, you can see that its state has changed to `SUSPEND`.

### Resume a pipe

Resume the pipe named `user_behavior_replica` in the database named `mydatabase`:

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica RESUME;
```

If you use [SHOW PIPES](../../../sql-reference/sql-statements/data-manipulation/SHOW_PIPES.md) to query the pipe, you can see that its state has changed to `RUNNING`.
