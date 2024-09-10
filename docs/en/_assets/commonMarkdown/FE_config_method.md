FE parameters are classified into dynamic parameters and static parameters.

- Dynamic parameters can be configured and adjusted by running SQL commands, which is very convenient. But the configurations become invalid if you restart your FE. Therefore, we recommend that you also modify the configuration items in the **fe.conf** file to prevent the loss of modifications.

- Static parameters can only be configured and adjusted in the FE configuration file **fe.conf**. **After you modify this file, you must restart your FE for the changes to take effect.**

Whether a parameter is a dynamic parameter is indicated by the `IsMutable` column in the output of [ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md). `TRUE` indicates a dynamic parameter.

Note that both dynamic and static FE parameters can be configured in the **fe.conf** file.
