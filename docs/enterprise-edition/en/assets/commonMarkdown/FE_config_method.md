FE parameters are classified into dynamic parameters and static parameters.

- Dynamic parameters can be configured and adjusted by running SQL commands, which is very convenient. But the configurations become invalid if you restart your FE. Therefore, we recommend that you also modify the configuration items in the **`starrocksFESpec.config`** section of `values.yaml` and restarting the FE nodes if they are already running.

- Static parameters can only be configured and adjusted in the **`starrocksFESpec.config`** section of `values.yaml`. **After you modify this file, you must restart your FE nodes for the changes to take effect.**

Whether a parameter is a dynamic parameter is indicated by the `IsMutable` column in the output of the SQL command `ADMIN SHOW CONFIG`. `TRUE` indicates a dynamic parameter.

Note that both dynamic and static FE parameters can be configured in the **`starrocksFESpec.config`** section of `values.yaml`.
