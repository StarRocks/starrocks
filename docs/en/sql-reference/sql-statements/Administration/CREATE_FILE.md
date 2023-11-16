---
displayed_sidebar: "English"
---

# CREATE FILE

You can execute the CREATE FILE statement to create a file. After a file is created, the file is uploaded and persisted in StarRocks. In a database, only an admin user can create and delete files, and all users who have permission to access the database can use the files that belong to the database.

## Basic concepts

**File**: refers to the file that is created and saved in StarRocks. After a file is created and stored in StarRocks, StarRocks assigns a unique ID to the file. You can find a file based on the database name, catalog, and file name.

## Syntax

```SQL
CREATE FILE "file_name" [IN database]
[properties]
```

## Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| file_name     | Yes          | The name of the file.                                        |
| database      | No           | The database to which the file belongs. If you do not specify this parameter, this parameter defaults to the name of the database that you access in the current session. |
| properties    | Yes          | The properties of the file. The following table describes the configuration items of properties. |

**Configuration items of** **`properties`**

| **Configuration item** | **Required** | **Description**                                              |
| ---------------------- | ------------ | ------------------------------------------------------------ |
| url                    | Yes          | The URL from which you can download the file. Only an unauthenticated HTTP URL is supported. After the file is stored in StarRocks, the URL is no longer needed. |
| catalog                | Yes          | The category to which the file belongs. You can specify a catalog based on your business requirements. However, in some situations, you must set this parameter to a specific catalog. For example, if you load data from Kafka, StarRocks searches for files in the catalog from the Kafka data source. |
| MD5                    | No           | The message-digest algorithm that is used to check a file. If you specify this parameter, StarRocks checks the file after the file is downloaded. |

## Examples

- Create a file named  **test.pem** under the category named kafka.

```SQL
CREATE FILE "test.pem"
PROPERTIES
(
    "url" = "https://starrocks-public.oss-cn-xxxx.aliyuncs.com/key/test.pem",
    "catalog" = "kafka"
);
```

- Create a file named **client.key** under the category named my_catalog.

```SQL
CREATE FILE "client.key"
IN my_database
PROPERTIES
(
    "url" = "http://test.bj.bcebos.com/kafka-key/client.key",
    "catalog" = "my_catalog",
    "md5" = "b5bb901bf10f99205b39a46ac3557dd9"
);
```
