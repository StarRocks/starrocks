# DROP FILE

## description

This statement is used to delete an uploaded file.

Syntax:

```sql
DROP FILE "file_name" [FROM database]
[properties]
```

Note:

```plain text
file_name:  File name
database: A db to which a file belongs. If not specified, db of the current session will be used. 

Properties support the following parameters: 
Catalog: mandatory. Classification of files. 
```

## example

1. Delete the file ca.pem

    ```sql
    DROP FILE "ca.pem" properties("catalog" = "kafka");
    ```

## keyword

DROP,FILE
