---
displayed_sidebar: docs
---

# Tableau

This topic describes how to connect StarRocks to Tableau Desktop and Tableau Server with StarRocks Tableau JDBC Connector.

## Overview

The StarRocks Tableau JDBC Connector is a custom extension for Tableau Desktop and Tableau Server. It simplifies the process of connecting Tableau to StarRocks and enhances support for standard Tableau functionality, outperforming the default Generic ODBC/JDBC connection.

### Key Features

- LDAP Support: Enables LDAP login with password prompts for secure authentication.
- High Compatibility: Achieves 99.99% compatibility in TDVT (Tableau Design Verification Tool) testing, with only one minor failure case.

## Prerequisites

Before proceeding, make sure the following requirements are met:

- Tableau Version: Tableau 2020.4 and later
- StarRocks Version: v3.2 and later

## Install Connector for Tableau Desktop

1. Download the [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/).
2. Store the driver file in the following directory (create the directory if it does not exist):

   - macOS: `~/Library/Tableau/Drivers`
   - Windows: `C:\Program Files\Tableau\Drivers`

3. Download the [StarRocks Tableau JDBC Connector file](https://releases.starrocks.io/resources/starrocks_jdbc-v1.2.0_signed.taco).
4. Store the connector file in the following directory:

   - macOS: `~/Documents/My Tableau Repository/Connectors`
   - Windows: `C:\Users\[Windows User]\Documents\My Tableau Repository\Connectors`

5. Launch Tableau Desktop.
6. Navigate to **Connect** -> **To a Server** -> **StarRocks JDBC by CelerData**.

## Install Connector for Tableau Server

1. Download the [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/).
2. Store the driver file in the following directory (create the directory if it does not exist):

   - Linux: `/opt/tableau/tableau_driver/jdbc`
   - Windows: `C:\Program Files\Tableau\Drivers`

   :::info

   On Linux, you must permit the "Tableau" user to access the directory.

   Follow these steps:

   1. Create the directory and copy the driver file to the directory:

      ```Bash
      sudo mkdir -p /opt/tableau/tableau_driver/jdbc

      # Replace <path_to_driver_file_name> with the absolute path of the driver file.
      sudo cp /<path_to_driver_file_name>.jar /opt/tableau/tableau_driver/jdbc
      ```
  
   2. Grant permission to the the "Tableau" user.

      ```Bash
      # Replace <driver_file_name> with the name of the driver file.
      sudo chmod 755 /opt/tableau/tableau_driver/jdbc/<driver_file_name>.jar
      ```

   :::

3. Download the [StarRocks Tableau JDBC Connector file](https://releases.starrocks.io/resources/starrocks_jdbc-v1.2.0_signed.taco).
4. Store the connector file in the following directory of each node:

   - Linux: `/opt/tableau/connectors`
   - Windows: `C:\Program Files\Tableau\Connectors`

5. Restart Tableau Server.

   ```Bash
   tsm restart
   ```

   :::info

   You must restart Tableau Server to apply the changes whenever you add, remove, or update a connector.

   :::

## Usage notes

If LDAP login support is required, you can tick the **Enable LDAP** switch in the **Advanced** tab during configuration.
