# SHOW FILE

You can execute the SHOW FILE statement to view the information about a file stored in a database.

## Syntax

```SQL
SHOW FILE [FROM database]
```

The file information returned by this statement is as follows:

- `FileId`: the globally unique ID of the file.

- `DbName`: the database to which the file belongs.

- `Catalog`: the category to which the file belongs.

- `FileName`: the name of the file.

- `FileSize`: the size of the file. The unit is bytes.

- `MD5`: the message-digest algorithm that is used to check the file.

## Examples

View the file stored in `my_database`.

```SQL
SHOW FILE FROM my_database;
```
