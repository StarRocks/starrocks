# SHOW PROCESSLIST

## Description

Lists the operations currently being performed by threads executing within the server. The current version of StarRocks only supports listing queries.

## Syntax

```SQL
SHOW [FULL] PROCESSLIST [from_clause]
```
or

```SQL
SHOW [FULL] PROCESSLIST FROM ALL;
```

or 

```SQL
SHOW [FULL] PROCESSLIST FROM 'fe_host';
```

## Parameter

| Parameter | Required | Description                                                                                                                                    |
|-----------| -------- |------------------------------------------------------------------------------------------------------------------------------------------------|
| FULL      | No       | If you specify this parameter, will output full SQL. Otherwise, only the first 100 chars of the sql are listed.                                |
| ALL       | No       | If you specify this parameter, the process of all frontend will be listed. Otherwise, only the process of the current frontend will be listed. |
| fe_host   | No       | If you specify this parameter, the process of specify frontend will be listed.  |

## Return

| Return        | Description                                                            |
|---------------|------------------------------------------------------------------------|
| Id            | Connection ID.                                                         |
| User          | The name of the user who runs the operation.                           |
| Host          | The hostname of the client which runs the operation.                   |
| Db            | The name of the database where the operation is executed.              |
| Command       | The type of the command.                                               |
| ConnectionStartTime | Time when the connection starts.                                       |
| Time          | The time (in second) since the operation has entered the current state. |
| State         | The state of the operation.                                            |
| Info          | The command that the operation is executing.                           |
| IsPending     | Whether the operation is being queued.                                 |
| FeHost        | the hostname of frontend.                                              |

## Usage note

If the current user is `root`, this statement lists the operations of all users in the cluster. Otherwise, only operations of the current user are listed.

## Example

Example 1: lists the operations state via the user `root`.

```Plain
SHOW PROCESSLIST;
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
| Id   | User | Host                | Db   | Command | ConnectionStartTime | Time | State | Info                      | IsPending | FeHost        |
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
|    0 | root | x.x.x.x:xxxx        | ssb  | Query   | 2022-11-13 21:18:19 |    0 | OK    | show processlist          | false     | x.x.x.1       |
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
```

Example 2: lists the operations state of all frontend via the user `root`.

```Plain
SHOW PROCESSLIST FROM ALL;
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
| Id   | User | Host                | Db   | Command | ConnectionStartTime | Time | State | Info                      | IsPending | FeHost        |
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
|    0 | root | x.x.x.x:xxxx        | ssb  | Query   | 2022-11-13 21:18:19 |    0 | OK    | show processlist          | false     | x.x.x.1       |
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
|    0 | root | x.x.x.x:xxxx        | ssb  | Sleep   | 2022-11-13 21:19:00 |    0 | OK    | show processlist from all | false     | x.x.x.2       |
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
```

Example 3: lists the operations state of specify frontend via the user `root`.

```Plain
SHOW PROCESSLIST FROM 'x.x.x.1';
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
| Id   | User | Host                | Db   | Command | ConnectionStartTime | Time | State | Info                      | IsPending | FeHost        |
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
|    0 | root | x.x.x.x:xxxx        | ssb  | Query   | 2022-11-13 21:18:19 |    0 | OK    | show processlist          | false     | x.x.x.1       |
+------+------+---------------------+------+---------+---------------------+------+-------+---------------------------+-----------+---------------+
```
