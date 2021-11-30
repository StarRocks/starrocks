# SHOW FILE

## description

The statement is used to show a file created in a database.

Syntax:

```sql
SHOW FILE [FROM database];
```

Note:

``` plain text
FileId: file Id, globally unique
Dbname: the name of the database to which it belongs 
Catalog: Custom categories 
Filename: file name 
FileSize: File size, byte as the unit
MD5: MD5 of the document
```

## example

1. View files uploaded from my_database.

    ```sql
    SHOW FILE FROM my_database;
    ```

## keyword

SHOW,FILE
