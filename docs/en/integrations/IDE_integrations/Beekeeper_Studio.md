---
sidebar_position: 5
displayed_sidebar: docs
description: "Beekeeper Studio is an open-source, cross-platform SQL editor and database manager with native StarRocks support."
---

# Beekeeper Studio

Beekeeper Studio is an [open-source](https://github.com/beekeeper-studio/beekeeper-studio), cross-platform SQL editor and database manager for Windows, macOS, and Linux with a native StarRocks driver.

![Beekeeper Studio connected to StarRocks](../../_assets/IDE_beekeeper_2.png)


Beekeeper Studio is an independent software business funded by sales of premium features in the app. It contains zero tracking and focuses heavily on ease-of-use.

StarRocks is included in Beekeeper Studio's free community edition, so you can browse schemas and run queries against StarRocks for free the same way you do with any other supported database.

## Prerequisites

Make sure that you have installed Beekeeper Studio 6.0 or later.
You can download Beekeeper Studio [from their website](https://www.beekeeperstudio.io/get).

## Usage

Follow these steps to connect to StarRocks:

1. Launch Beekeeper Studio.

2. On the connection screen, select **StarRocks** from the **Connection Type** drop-down list.

   ![Beekeeper Studio - Connection screen](../../_assets/IDE_beekeeper_1.png)

3. Configure the following connection settings:

   - **Host**: the FE host IP address of your StarRocks cluster.
   - **Port**: the FE query port of your StarRocks cluster, for example, `9030`.
   - **User**: the username that is used to log in to your StarRocks cluster, for example, `admin`.
   - **Password**: the password that is used to log in to your StarRocks cluster.
   - **Default Database**: (optional) the database to use after connecting.

4. Click **Test** to verify the accuracy of the connection settings, and then click **Connect**. You can also name and save the connection under **Save Connection** for later reuse.

5. After the connection is established, you can browse databases and tables in the left-side sidebar, and write and run queries in the SQL editor.

   ![Beekeeper Studio - SQL editor](../../_assets/IDE_beekeeper_2.png)
