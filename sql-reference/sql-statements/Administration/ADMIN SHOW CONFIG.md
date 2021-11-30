# ADMIN SHOW CONFIG

## description

This statement is used to show the configuration of the current cluster (currently only FE configuration items are supported).

Syntax:

```sql
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

Note:

The implications of the results are as follows:

```plain text
1. Key：        Configuration item name
2. Value：      Configuration item value
3. Type：       Configuration item type 
4. IsMutable：  Whether it could be set through ADMIN SET CONFIG command
5. MasterOnly： Whether it only applies to MASTER FE
6. Comment：    Configuration item description 
```

## example

1. View the configuration of the current FE node

    ```sql
    ADMIN SHOW FRONTEND CONFIG;
    ```

2. Search for the configuration of the current FE node by using like predicate  

    ```plain text
    mysql> ADMIN SHOW FRONTEND CONFIG LIKE '%check_java_version%';
    +--------------------+-------+---------+-----------+------------+---------+
    | Key                | Value | Type    | IsMutable | MasterOnly | Comment |
    +--------------------+-------+---------+-----------+------------+---------+
    | check_java_version | true  | boolean | false     | false      |         |
    +--------------------+-------+---------+-----------+------------+---------+
    1 row in set (0.00 sec)
    ```

## keyword

ADMIN,SHOW,CONFIG
