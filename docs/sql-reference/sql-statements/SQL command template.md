# SQL statement template

> *This template uses* `*ADMIN SET REPLICA STATUS*` *as an example to illustrate the requirements for writing SQL command topics.*

- > *Capitalize commands and keywords in the running text. For example, "The SELECT statement is used to query records that meet specific conditions.", "You can use GROUP BY to group data in this column.", "The LIMIT keyword specifies the maximum number of records that can be returned".*

- > *If you need to refer to a parameter or parameter value in the running text, enclose it in two backticks (``), for example,* `*cachesize*`*.*

## ADMIN SET REPLICA STATUS

> *The topic title. Use the English command* *name as the topic title. Capitalize all the letters in the command* *name. Make sure you use the correct spelling.*

### Description

Specifies the replica status of a tablet. This command is used to manually set the replica status of tablet to `bad` or `ok`.

> *What does this command do. You can add related descriptions or usage notes.*

### Syntax

```SQL
ADMIN SET REPLICA STATUS

PROPERTIES ("key" = "value", ...);
```

> *The syntax of this command. Enclose syntax in a code block. Make sure that the syntax complies with coding specifications.*

- > *Use proper line wrap and indentation.*

- > *Do not use Chinese characters in the code, such as Chinese semicolons or comas.*

- > *Capitalize the keywords in an SQL command. Example:*

```SQL
SELECT ta.x, count(ta.y) AS y, sum(tb.z) AS z

FROM (

    SELECT a AS x, b AS y

    FROM t) ta

    JOIN tb

        ON ta.x = tb.x

WHERE tb.a > 10

GROUP BY ta.x

ORDER BY ta.x, z

LIMIT 10
```

### Parameters

`PROPERTIES`: Each property must be a key-value pair. Supported properties:

- `tablet_id`: the ID of the tablet. This parameter is required.
- `backend_id`: the BE ID of the tablet. This parameter is required.
- `status`: the status of replicas. This parameter is required. Valid values: `bad` and `ok`. The value `ok` indicates that the system automatically repairs the replicas of a tablet. If the replica status is set to `bad`, the replicas may be immediately deleted. Exercise caution when you perform this operation. If the tablet you specified does not exist or the replica status is `bad`, the system ignores these replicas.

> *Description of parameters in a command.*

- > *A preferred parameter description must include the parameter meaning, value format, value range, whether this parameter is required, and other remarks if needed.*

- > You can use an unordered list to organize parameter description. If the description is complex, you can organize information as a table. The table can consist of the following columns: parameter name, value type (optional), example value (optional), parameter description.

### Usage notes (optional)

> You can *add some notes or precautions for using this command.*

### Examples

Example 1: Set the replica status of tablet 10003 on BE 10001 to `bad`.

```SQL
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");
```

Example 2: Set the replica status of tablet 10003 on BE 10001 to `ok`.

```SQL
ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "ok");
```

- > *Provide examples for using this command and explain the purpose of each example.*

- > *You can provide multiple examples.*

- > *If you need to describe more than one scenario in an example, add a comment for each scenario in the code snippet to help* *users quickly distinguish between them.*
