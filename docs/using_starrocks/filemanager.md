# File manager

With file manager, you can create, view, and delete files, such as the files that are used to access external data sources: public key files, private key files, and certificate files. You can reference or access the created files by using commands.

## Basic concepts

**File**: refers to the file that is created and saved in StarRocks. After a file is created and stored in StarRocks, StarRocks assigns a unique ID to the file. You can find a file based on the database name, catalog, and file name. In a database, only an admin user can create and delete files, and all users who have permissions to access a database can use the files that belong to the database.

## Before you begin

- Configure the following parameters for each frontend (FE).
  - `small_file_dir`: the path in which the uploaded files are stored. The default path is `small_files/`, which is in the runtime directory of the FE. You need to specify this parameter in the **fe.conf** file and then restart the FE.
  - `max_small_file_size_bytes`: the maximum size of a single file. The default value of this parameter is 1 MB. If the size of a file exceeds the value of this parameter, the file cannot be created.
  - `max_small_file_number`: the maximum number of files that can be created within a cluster. The default value of this parameter is 100. If the number of files that you have created reaches the value of this parameter, you cannot continue creating files. You can specify this parameter by using the ADMIN SET CONFIG statement.

> Note: Increasing the values of the two parameters causes an increase in the memory usage of the FE. Therefore, we recommend that you do not increase the values of the two parameters unless necessary.

- Configure the following parameters for each backend (BE).

`small_file_dir`: the path in which the downloaded files are stored. The default path is `lib/small_files/`, which is in the runtime directory of the BE. You can specify this parameter in the **be.conf** file.
