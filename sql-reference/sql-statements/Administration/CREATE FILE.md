# CREATE FILE

## description

This statement is used to create and upload a file to the StarRocks cluster. This function is often used to manage files that need to be used in other commands, such as certificates, public keys and private keys, etc.

This command can only be excuted by users with admin privileges. A file belongs to a database. Users who have access to this database can use the file.

The size of a single file is limited to 1MB.

A StarRocks cluster uploads up to 100 files.

Syntax:

```sql
CREATE FILE "file_name" [IN database]
[properties]
```

Note:

```plain text
file_name: Custom file name
database: The file belongs to a db. If not specified, the db of the current session will be used. 
Properties support the following parameters: 

Url: Mandatory. Specify the download path of a file. Currently, only unauthenticated HTTP download paths are supported. After the command is successfully executed, the file will be stored in StarRocks and url is no longer needed. 
Catalog: Mandatory. Custom the type name of the file. Some commands will look for files in specified catalog. For example, in a routine import, when the data source is kafka, files under the catalog named kafka will be looked up.  
md5: Optional. Md5 of the file. If specified, it will be checked after downloading the file. 
```

## example

1. Create a file ca.pem and categorize it as kafka.

    ```sql
    CREATE FILE "ca.pem"
    PROPERTIES
    (
        "url" = "https://test.bj.bcebos.com/kafka-key/ca.pem",
        "catalog" = "kafka"
    );
    ```

2. Create a file client.key and categorize it as my_catalog.

    ```sql
    CREATE FILE "client.key"
    IN my_database
    PROPERTIES
    (
        "url" = "https://test.bj.bcebos.com/kafka-key/client.key",
        "catalog" = "my_catalog",
        "md5" = "b5bb901bf10f99205b39a46ac3557dd9"
    );
    ```

## keyword

CREATE,FILE
