# File manager

With file manager, you can create, view, and delete files, such as the files that are used to access external data sources: public key files, private key files, and certificate files. You can reference or access the created files by using commands.

## Basic concepts

**File**: refers to the file that is created and saved in StarRocks. After a file is created and stored in StarRocks, StarRocks assigns a unique ID to the file. You can find a file based on database name, catalog, and file name. In a database, only an admin user can create and delete files, and all users who have permissions to access a database can use the files that belong to the database.

## Before you begin

- Configure the following parameters for each frontend (FE).

  - `small_file_dir`: the path in which the uploaded files are stored. The default path is `small_files/`, which is in the runtime directory of the FE.

  - `max_small_file_size_bytes`: the maximum size of a single file. The default value of this parameter is 1 MB. If the size of a file exceeds the value of this parameter, the file cannot be created.

  - `max_small_file_number`: the maximum number of files that can be created within a cluster. The default value of this parameter is 100. If the number of files that you have created reaches the value of this parameter, you cannot continue creating files.

    You can execute the ADMIN SET CONFIG statement to modify the values of `max_small_file_size_bytes` and `max_small_file_number`. Increasing the values of the two parameters causes an increase in the memory usage of the FE. Therefore, we recommend that you do not increase the values of the two parameters.

- Configure the following parameters for each backend (BE).

  `small_file_dir`: the path in which the downloaded files are stored. The default path is `lib/small_files/`, which is in the of runtime directory of the BE.

## Supported operations

### Create a file

You can execute the CREATE FILE statement to create a file, upload the file, and store the file in StarRocks. After a file is created and stored in StarRocks, StarRocks persists all data related to the file.

#### Syntax

```SQL
CREATE FILE "file_name" [IN database]

[properties]
```

#### Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| file_name     | Yes          | The name of the file.                                        |
| database      | No           | The database to which the file belongs. If you do not specify this parameter, this parameter defaults to the name of the database that you access in the current session. |
| properties    | Yes          | The properties of the file. The following table describes the child parameters of properties. |

Child parameters of `properties`

| **Child parameter** | **Required** | **Description**                                              |
| ------------------- | ------------ | ------------------------------------------------------------ |
| url                 | Yes          | The URL from which you can download the file. Only an unauthenticated HTTP URL is supported. After the file is stored in StarRocks, the URL is no longer needed. |
| catalog             | Yes          | The category to which the file belongs. You can specify a catalog based on your business requirements. However, in some situations, you must set this parameter to a specific catalog. For example, if you load data from Kafka, StarRocks searches for files in the catalog from the Kafka data source. |
| MD5                 | No           | The message-digest algorithm that is used to check a file. If you specify this parameter, StarRoocks checks the file after the file is dowloaded. |

#### Examples

- Create a file named **ca.pem** under the category named **kafka**.

```SQL
CREATE FILE "ca.pem"

PROPERTIES

(

    "url" = "http://test.bj.bcebos.com/kafka-key/ca.pem",

    "catalog" = "kafka"

);
```

- Create a file named **client.key** under the category named **my_catelog**.

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

### View a file

You can execute the SHOW FILE statement to view the information about a file stored in a database.

#### Syntax

```SQL
SHOW FILE [FROM database];
```

The information is as follows:

- `FileId`: the globally unique ID of the file.
- `DbName`: the database to which the file belongs.
- `Catalog`: the category to which the file belongs.
- `FileName`: the name of the file.
- `FileSize`: the size of the file.
- `MD5`: the message-digest algorithm that is used to check the file.

#### Examples

View the file in the database named **mydatabase**.

```SQL
SHOW FILE FROM mydatabase;
```

### Delete a file

You can execute the DROP FILE statement to delete a file.

#### Syntax

```SQL
DROP FILE "file_name" [FROM database]

[properties]
```

#### Parameters

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| file_name     | Yes          | The name of the file                                         |
| database      | No           | The database to which a file belongs.                        |
| properties    | Yes          | The properties of the file. The following table describes the child parameter of properties. |

Child parameters of `properties`

| **Child parameter** | **Required** | **Description**                       |
| ------------------- | ------------ | ------------------------------------- |
| catalog             | Yes          | The category to which a file belongs. |

#### Examples

Delete a file named **ca.pem**.

```SQL
DROP FILE "ca.pem" properties("catalog" = "kafka");
```

## How it works

- How to create and delete a file

  When you execute the CREATE FILE statement to create a file, the FE downloads the file to its memory and stores it in the BASE64 format. Then the FE persists the file into Berkeley DB Java Edition (BDBJE). If the FE breaks down and restarts, it loads the file from BDBJE into its memory. If the file is deleted, it is deleted from both the FE and BDBJE.

- How a BE and an FE use files
  - When an FE uses a file, the SmallFileMgr class stores the data related to the file in the specified directory of the FE. Then the SmallFileMgr class returns a local file path for the FE to use the file.
  - When a BE uses a file, the BE calls the **/api/get_small_file** API (HTTP) to download the file to its specified directory and record the information of the file. When the BE requests to use the file, the BE checks whether the file has been downloaded and then verifies the file. If the file pass the verification, the path of the file is returned. If the file fails the verification, the file is deleted and re-downloaded from the FE. When a BE restarts, it preloads the downloaded file into its memory.
  