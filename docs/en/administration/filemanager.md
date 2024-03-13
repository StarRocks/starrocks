---
displayed_sidebar: "English"
---

# File manager

With file manager, you can create, view, and delete files, such as the files that are used to access external data sources: public key files, private key files, and certificate files. You can reference or access the created files by using commands.

## Basic concepts

**File**: refers to the file that is created and saved in StarRocks. After a file is created and stored in StarRocks, StarRocks assigns a unique ID to the file. You can find a file based on the database name, catalog, and file name. In a database, only an admin user can create and delete files, and all users who have permissions to access a database can use the files that belong to the database.

## Before you begin

- Configure the following parameters for each FE.
  - `small_file_dir`: the path in which the uploaded files are stored. The default path is `small_files/`, which is in the runtime directory of the FE. You need to specify this parameter in the **fe.conf** file and then restart the FE to allow the change to take effect.
  - `max_small_file_size_bytes`: the maximum size of a single file. The default value of this parameter is 1 MB. If the size of a file exceeds the value of this parameter, the file cannot be created. You can specify this parameter by using the [ADMIN SET CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) statement.
  - `max_small_file_number`: the maximum number of files that can be created within a cluster. The default value of this parameter is 100. If the number of files that you have created reaches the value of this parameter, you cannot continue creating files. You can specify this parameter by using the ADMIN SET CONFIG statement.

> Note: Increasing the values of the two parameters causes an increase in the memory usage of the FE. Therefore, we recommend that you do not increase the values of the two parameters unless necessary.

- Configure the following parameters for each BE.

`small_file_dir`: the path in which the downloaded files are stored. The default path is `lib/small_files/`, which is in the runtime directory of the BE. You can specify this parameter in the **be.conf** file.

## Create a file

You can execute the CREATE FILE statement to create a file. For more information, see [CREATE FILE](../sql-reference/sql-statements/Administration/CREATE_FILE.md). After a file is created, the file is uploaded and persisted in StarRocks.

## View a file

You can execute the SHOW FILE statement to view the information about a file stored in a database. For more information, see [SHOW FILE](../sql-reference/sql-statements/Administration/SHOW_FILE.md).

## Delete a file

You can execute the DROP FILE statement to delete a file. For more information, see [DROP FILE](../sql-reference/sql-statements/Administration/DROP_FILE.md).

## How an FE and a BE use a file

- **FE**: The SmallFileMgr class stores the data related to the file in the specified directory of the FE. Then the SmallFileMgr class returns a local file path for the FE to use the file.
- **BE**: The BE calls the **/api/get_small_file API** (HTTP) to download the file to its specified directory and record the information of the file. When the BE requests to use the file, the BE checks whether the file has been downloaded and then verifies the file. If the file pass the verification, the path of the file is returned. If the file fails the verification, the file is deleted and re-downloaded from the FE. When a BE restarts, it preloads the downloaded file into its memory.
