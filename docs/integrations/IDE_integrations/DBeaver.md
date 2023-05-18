# DBeaver

DBeaver is a SQL client software application and a database administration tool, which offers a helpful assistant that walks you through the process of connecting to a database.

## Prerequisites

Make sure that you have installed DBeaver.

You can download the DBeaver Community edition at [https://dbeaver.io](https://dbeaver.io/) or the DBeaver PRO edition at [https://dbeaver.com](https://dbeaver.com/).

## Integration

Follow these steps to connect to a database:

1. Launch DBeaver.

2. Click the plus sign (**+**) icon in the upper-left corner of the DBeaver window or choose **Database** > **New Database Connection** in the menu bar to access the assistant.

   ![DBeaver - Access the assistant](../../assets/IDE_dbeaver_1.png)

   ![DBeaver - Access the assistant](../../assets/IDE_dbeaver_2.png)

3. Select the MySQL driver.

   In the **Select your database** step, you are presented with a list of available drivers. Click **Analytical** in the left-side pane to quickly locate the MySQL driver. Then, double-click the **MySQL** icon.

   ![DBeaver - Select your database](../../assets/IDE_dbeaver_3.png)

4. Configure the connection to the database.

   In the **Connection Settings** step, go to the **Main** tab and configure the following essential connection settings:

   - **Server Host**: the FE host IP address of your StarRocks cluster.
   - **Port**: the FE query port of your StarRocks cluster, for example, `9030`.
   - **Database**: the target database in your StarRocks cluster. Both internal and external databases are supported, but the functionality for external databases may be incomplete.
   - **Username**: the username that is used to log in to your StarRocks cluster, for example, `admin`.
   - **Password**: the password that is used to log in to your StarRocks cluster.

   ![DBeaver - Connection Settings - Main tab](../../assets/IDE_dbeaver_4.png)

   You can also view and edit the properties of the MySQL driver on the **Driver properties** tab if necessary. To edit a specific property, click the row in the **Value** column for that property.

   ![DBeaver - Connection Settings - Driver properties tab](../../assets/IDE_dbeaver_5.png)

5. Test the connection to the database.

   Click **Test Connection** to verify the accuracy of the connection settings. A dialog box displaying the MySQL driver's information appears. Click **OK** in the dialog box to confirm the information. After you have successfully configured the connection settings, click **Finish** to complete the process.

   ![DBeaver - Test Connection](../../assets/IDE_dbeaver_6.png)

6. Connect to the database.

   After the connection is established, you can view it in the left-side database connection tree and DBeaver can effectively connect to the database.

   ![DBeaver - Connect database](../../assets/IDE_dbeaver_7.png)
