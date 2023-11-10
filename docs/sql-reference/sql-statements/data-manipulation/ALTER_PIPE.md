# ALTER PIPE

## Description

Alters the settings of the properties of a pipe.

## Syntax

```SQL
ALTER PIPE [db_name.]<pipe_name> 
SET PROPERTY
(
    "<key>" = <value>[, "<key>" = "<value>" ...]
) 
```

## Parameters

### db_name

The name of the database to which the pipe belongs.

### pipe_name

The name of the pipe.

### PROPERTIES

The properties whose settings you want to alter for the pipe. Format: `"key" = "value"`. For more information about the properties supported, see [CREATE PIPE](../../../sql-reference/sql-statements/data-manipulation/CREATE_PIPE.md).

## Examples

Change the setting of the `AUTO_INGEST` property to `FALSE` for the pipe named `user_behavior_replica` in the database named `mydatabase`:

```SQL
USE mydatabase;
ALTER PIPE user_behavior_replica
SET
(
    "AUTO_INGEST" = "FALSE"
);
```
