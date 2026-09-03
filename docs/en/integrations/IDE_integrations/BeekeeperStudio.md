---
sidebar_position: 5
displayed_sidebar: docs
description: "Beekeeper Studio is a modern, easy-to-use SQL client and database manager with built-in support for connecting to StarRocks."
---

# Beekeeper Studio

[Beekeeper Studio](https://www.beekeeperstudio.io/) is a free, open-source SQL editor and database manager with a modern, easy-to-use interface. It is available for Windows, macOS, and Linux, and also offers a paid Ultimate tier with cloud sync and team-sharing features.

Beekeeper Studio has built-in support for connecting directly to StarRocks.

## Prerequisites

Make sure that you have installed Beekeeper Studio.

You can download Beekeeper Studio at [https://www.beekeeperstudio.io/get](https://www.beekeeperstudio.io/get).

## Integration

Follow these steps to connect to a StarRocks cluster:

1. Launch Beekeeper Studio.

2. On the connections screen, click **New Connection**.

   ![Beekeeper Studio - New Connection](../../_assets/IDE_beekeeperstudio_1.png)

3. Select **StarRocks** as the connection type.

   In the connection type dropdown, search for or select **StarRocks**.

   ![Beekeeper Studio - Select StarRocks connection type](../../_assets/IDE_beekeeperstudio_2.png)

4. Configure the connection to the database.

   Fill in the following connection settings:

   - **Host**: the FE host IP address of your StarRocks cluster.
   - **Port**: the FE query port of your StarRocks cluster, for example, `9030`.
   - **User**: the username that is used to log in to your StarRocks cluster, for example, `root`.
   - **Password**: the password that is used to log in to your StarRocks cluster.
   - **Default Database**: (optional) the target database in your StarRocks cluster.

   :::tip
   You can also click **Import from URL** and paste a full connection URL (for example, `mysql://root@<host>:9030`), and Beekeeper Studio will parse it into the fields above automatically.
   :::

   ![Beekeeper Studio - Connection settings](../../_assets/IDE_beekeeperstudio_3.png)

5. Test the connection to the database.

   Click **Test** to verify the accuracy of the connection settings.

   ![Beekeeper Studio - Test connection](../../_assets/IDE_beekeeperstudio_4.png)

6. Connect to the database.

   Click **Connect** to save the connection and connect to your StarRocks cluster. Once connected, you can browse databases, tables, and views in the left-side sidebar, and run SQL queries against your StarRocks cluster.

   ![Beekeeper Studio - Connected to StarRocks](../../_assets/IDE_beekeeperstudio_5.png)
