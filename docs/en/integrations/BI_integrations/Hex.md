---
displayed_sidebar: docs
---

# Hex

Hex supports querying and visualizing both internal data and external data in StarRocks.

Add a data connection in Hex. Note that you must select MySQL as the connection type.

![Hex](../../_assets/BI_hex_1.png)

The parameters that you need to configure are described as follows:

- **Name**: the name of the data connection.
- **Host & port**: the FE host IP address and FE query port of your StarRocks cluster. An example query port is `9030`.
- **Database**: the data source that you want to access in your StarRocks cluster. The value of this parameter is in the `<catalog_name>.<database_name>` format.
  - `catalog_name`: the name of the target catalog in your StarRocks cluster. Both internal and external catalogs are supported.
  - `database_name`: the name of the target database in your StarRocks cluster. Both internal and external databases are supported.
- **Type**: the authentication method that you want to use. Select **Password**.
- **User**: the username that is used to log in to your StarRocks cluster, for example, `admin`.
- **Password**: the password that is used to log in to your StarRocks cluster.
