# DELETE

## description

This statement is used to conditionally delete data from specified table (base index) partition.

This operation also deletes the rollup index data related to the base index at the same time.

Syntax:

```sql
DELETE FROM table_name [PARTITION partition_name]
WHERE
column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
```

Note:

1. Optional types of op include: `=, >, <, >=, <=, <=, <=, !=, in, not in`
2. Only the conditions on key columns can be specified.
3. When the selected key column does not exist in a certain rollup, delete cannot be performed.
4. The relationship between conditions can only be "and". If you want to achieve the "or" relationship, you need to divide the conditions into two DELETE statements.

Notice:

This statement may reduce query efficiency for a period of time after execution. The degree of impact depends on the number of delete conditions specified in the statement. The more conditions specified, the greater the impact.

## example

1. Delete data rows whose k1 column value is 3 in my_table partition p1

    ```sql
    DELETE FROM my_table PARTITION p1
    WHERE k1 = 3;
    ```

2. Delete data rows whose k1 column value is greater than or equal to 3 and whose k2 column value is "abc" in my_table partition p1

    ```sql
    DELETE FROM my_table PARTITION p1
    WHERE k1 >= 3 AND k2 = "abc";
    ```

3. Delete data rows whose k2 column value is "abc" or "cba"in all partitions of my_table partition p1,p2

    ```sql
    DELETE FROM my_table
    WHERE  k2 in ("abc", "cba");
    ```

## keyword

DELETE
