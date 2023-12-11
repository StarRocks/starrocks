---
displayed_sidebar: "English"
---

# columns

`columns` contains information about all table columns (or view columns).

The following fields are provided in `columns`:

| **Field**                | **Description**                                              |
| ------------------------ | ------------------------------------------------------------ |
| TABLE_CATALOG            | The name of the catalog to which the table containing the column belongs. This value is always `NULL`. |
| TABLE_SCHEMA             | The name of the database to which the table containing the column belongs. |
| TABLE_NAME               | The name of the table containing the column.                 |
| COLUMN_NAME              | The name of the column.                                      |
| ORDINAL_POSITION         | The ordinal position of the column within the table.         |
| COLUMN_DEFAULT           | The default value for the column. This is `NULL` if the column has an explicit default of `NULL`, or if the column definition includes no `DEFAULT` clause. |
| IS_NULLABLE              | The column nullability. The value is `YES` if `NULL` values can be stored in the column, `NO` if not. |
| DATA_TYPE                | The column data type. The `DATA_TYPE` value is the type name only with no other information. The `COLUMN_TYPE` value contains the type name and possibly other information such as the precision or length. |
| CHARACTER_MAXIMUM_LENGTH | For string columns, the maximum length in characters.        |
| CHARACTER_OCTET_LENGTH   | For string columns, the maximum length in bytes.             |
| NUMERIC_PRECISION        | For numeric columns, the numeric precision.                  |
| NUMERIC_SCALE            | For numeric columns, the numeric scale.                      |
| DATETIME_PRECISION       | For temporal columns, the fractional seconds precision.      |
| CHARACTER_SET_NAME       | For character string columns, the character set name.        |
| COLLATION_NAME           | For character string columns, the collation name.            |
| COLUMN_TYPE              | The column data type.<br />The `DATA_TYPE` value is the type name only with no other information. The `COLUMN_TYPE` value contains the type name and possibly other information such as the precision or length. |
| COLUMN_KEY               | Whether the column is indexed:<ul><li>If `COLUMN_KEY` is empty, the column either is not indexed or is indexed only as a secondary column in a multiple-column, nonunique index.</li><li>If `COLUMN_KEY` is `PRI`, the column is a `PRIMARY KEY` or is one of the columns in a multiple-column `PRIMARY KEY`.</li><li>If `COLUMN_KEY` is `UNI`, the column is the first column of a `UNIQUE` index. (A `UNIQUE` index permits multiple `NULL` values, but you can tell whether the column permits `NULL` by checking the `Null` column.)</li><li>If `COLUMN_KEY` is `DUP`, the column is the first column of a nonunique index in which multiple occurrences of a given value are permitted within the column.</li></ul>If more than one of the `COLUMN_KEY` values applies to a given column of a table, `COLUMN_KEY` displays the one with the highest priority, in the order `PRI`, `UNI`, `DUP`.<br />A `UNIQUE` index may be displayed as `PRI` if it cannot contain `NULL` values and there is no `PRIMARY KEY` in the table. A `UNIQUE` index may display as `MUL` if several columns form a composite `UNIQUE` index; although the combination of the columns is unique, each column can still hold multiple occurrences of a given value. |
| EXTRA                    | Any additional information that is available about a given column. |
| PRIVILEGES               | The privileges you have for the column.                      |
| COLUMN_COMMENT           | Any comment included in the column definition.               |
| COLUMN_SIZE              |                                                              |
| DECIMAL_DIGITS           |                                                              |
| GENERATION_EXPRESSION    | For generated columns, displays the expression used to compute column values. Empty for nongenerated columns. |
| SRS_ID                   | This value applies to spatial columns. It contains the column `SRID` value that indicates the spatial reference system for values stored in the column. |
