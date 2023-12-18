---
displayed_sidebar: "English"
---

# events

`events` provides information about Event Manager events.

The following fields are provided in `events`:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| EVENT_CATALOG        | The name of the catalog to which the event belongs. This value is always `def`. |
| EVENT_SCHEMA         | The name of the database to which the event belongs.         |
| EVENT_NAME           | The name of the event.                                       |
| DEFINER              | The user named in the `DEFINER` clause (often the user who created the event). |
| TIME_ZONE            | The event time zone, which is the time zone used for scheduling the event and that is in effect within the event as it executes. The default value is `SYSTEM`. |
| EVENT_BODY           | The language used for the statements in the event's `DO` clause. The value is always `SQL`. |
| EVENT_DEFINITION     | The text of the SQL statement making up the event's `DO` clause; in other words, the statement executed by this event. |
| EVENT_TYPE           | The event repetition type, either `ONE TIME` (transient) or `RECURRING` (repeating). |
| EXECUTE_AT           | For a one-time event, this is the `DATETIME` value specified in the `AT` clause of the `CREATE EVENT` statement used to create the event, or of the last `ALTER EVENT` statement that modified the event. The value shown in this column reflects the addition or subtraction of any `INTERVAL` value included in the event's `AT` clause. For example, if an event is created using `ON SCHEDULE AT CURRENT_DATETIME + '1:6' DAY_HOUR`, and the event was created at 2018-02-09 14:05:30, the value shown in this column would be `'2018-02-10 20:05:30'`. If the event's timing is determined by an `EVERY` clause instead of an `AT` clause (that is, if the event is recurring), the value of this column is `NULL`. |
| INTERVAL_VALUE       | For a recurring event, the number of intervals to wait between event executions. For a transient event, the value is always `NULL`. |
| INTERVAL_FIELD       | The time units used for the interval which a recurring event waits before repeating. For a transient event, the value is always `NULL`. |
| SQL_MODE             | The SQL mode in effect when the event was created or altered, and under which the event executes. |
| STARTS               | The start date and time for a recurring event. This is displayed as a `DATETIME` value, and is `NULL` if no start date and time are defined for the event. For a transient event, this column is always `NULL`. For a recurring event whose definition includes a `STARTS` clause, this column contains the corresponding `DATETIME` value. As with the `EXECUTE_AT` column, this value resolves any expressions used. If there is no `STARTS` clause affecting the timing of the event, this column is `NULL`. |
| ENDS                 | For a recurring event whose definition includes a `ENDS` clause, this column contains the corresponding `DATETIME` value. As with the `EXECUTE_AT` column, this value resolves any expressions used. If there is no `ENDS` clause affecting the timing of the event, this column is `NULL`. |
| STATUS               | The event status. One of `ENABLED`, `DISABLED`, or `SLAVESIDE_DISABLED`. `SLAVESIDE_DISABLED` indicates that the creation of the event occurred on another MySQL server acting as a replication source and replicated to the current MySQL server which is acting as a replica, but the event is not presently being executed on the replica. |
| ON_COMPLETION        | Valid values: `PRESERVE` and `NOT PRESERVE`.                 |
| CREATED              | The date and time when the event was created. This is a `DATETIME` value. |
| LAST_ALTERED         | The date and time when the event was last modified. This is a `DATETIME` value. If the event has not been modified since its creation, this value is the same as the `CREATED` value. |
| LAST_EXECUTED        | The date and time when the event last executed. This is a `DATETIME` value. If the event has never executed, this column is `NULL`.`LAST_EXECUTED` indicates when the event started. As a result, the `ENDS` column is never less than `LAST_EXECUTED`. |
| EVENT_COMMENT        | The text of the comment, if the event has one. If not, this value is empty. |
| ORIGINATOR           | The server ID of the MySQL server on which the event was created; used in replication. This value may be updated by `ALTER EVENT` to the server ID of the server on which that statement occurs, if executed on a replication source. The default value is 0. |
| CHARACTER_SET_CLIENT | The session value of the `character_set_client` system variable when the event was created. |
| COLLATION_CONNECTION | The session value of the `collation_connection` system variable when the event was created. |
| DATABASE_COLLATION   | The collation of the database with which the event is associated. |
