---
displayed_sidebar: docs
---

# Post-deployment setup

This topic describes tasks that you should perform after deploying StarRocks.

Before getting your new StarRocks cluster into production, you must secure the initial account and set the necessary variables and properties to allow your cluster to run properly.

## Secure initial account

Upon the creation of a StarRocks cluster, the initial `root` user of the cluster is generated automatically. The `root` user is granted the `root` privileges, which are the collection of all privileges within the cluster. We recommend you secure this user account and avoid using it in production to prevent misuse.

StarRocks automatically assigns an empty password to the `root` user when the cluster is created. Follow these procedures to set a new password for the `root` user:

1. Connect to StarRocks via your MySQL client with the username `root` and an empty password.

   ```Bash
   # Replace <fe_address> with the IP address (priority_networks) or FQDN 
   # of the FE node you connect to, and replace <query_port> 
   # with the query_port (Default: 9030) you specified in fe.conf.
   mysql -h <fe_address> -P<query_port> -uroot
   ```

2. Reset the password of the `root` user by executing the following SQL:

   ```SQL
   -- Replace <password> with the password you want to assign to the root user.
   SET PASSWORD = PASSWORD('<password>')
   ```

> **NOTE**
>
> - Keep the password properly after resetting it. If you forgot the password, see [Reset lost root password](../administration/user_privs/User_privilege.md#reset-lost-root-password) for detailed instructions.
> - After completing the post-deployment setup, you can create new users and roles to manage the privileges within your team. See [Manage user privileges](../administration/user_privs/User_privilege.md) for detailed instructions.

## Set necessary system variables

To allow your StarRocks cluster to work properly in production, you need to set the following system variables:

| **Variable name**                   | **StarRocks Version** | **Recommended value**                                        | **Description**                                              |
| ----------------------------------- | --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| is_report_success                   | v2.4 or earlier       | false                                                        | The boolean switch that controls whether to send the profile of a query for analysis. The default value is `false`, which means no profile is required. Setting this variable to `true` can affect the concurrency of StarRocks. |
| enable_profile                      | v2.5 or later         | false                                                        | The boolean switch that controls whether to send the profile of a query for analysis. The default value is `false`, which means no profile is required. Setting this variable to `true` can affect the concurrency of StarRocks. |
| enable_pipeline_engine              | v2.3 or later         | true                                                         | The boolean switch that controls whether to enable the pipeline execution engine. `true` indicates enabled and `false` indicates the opposite. Default value: `true`. |
| parallel_fragment_exec_instance_num | v2.3 or later         | If you have enabled the pipeline engine, you can set this variable to `1`.If you have not enabled the pipeline engine, you should set it to half the number of CPU cores. | The number of instances used to scan nodes on each BE. The default value is `1`. |
| pipeline_dop                        | v2.3, v2.4, and v2.5  | 0                                                            | The parallelism of a pipeline instance, which is used to adjust the query concurrency. Default value: `0`, indicating the system automatically adjusts the parallelism of each pipeline instance.<br />From v3.0 onwards, StarRocks adaptively adjusts this parameter based on query parallelism. |

- Set `is_report_success` to `false` globally:

  ```SQL
  SET GLOBAL is_report_success = false;
  ```

- Set `enable_profile` to `false` globally:

  ```SQL
  SET GLOBAL enable_profile = false;
  ```

- Set `enable_pipeline_engine` to `true` globally:

  ```SQL
  SET GLOBAL enable_pipeline_engine = true;
  ```

- Set `parallel_fragment_exec_instance_num` to `1` globally:

  ```SQL
  SET GLOBAL parallel_fragment_exec_instance_num = 1;
  ```

- Set `pipeline_dop` to `0` globally:

  ```SQL
  SET GLOBAL pipeline_dop = 0;
  ```

For more information about system variables, see [System variables](../sql-reference/System_variable.md).

## Set user property

If you have created new users in your cluster, you need to enlarge their maximum connection number (to `1000`, for example):

```SQL
-- Replace <username> with the username you want to enlarge the maximum connection number for.
SET PROPERTY FOR '<username>' 'max_user_connections' = '1000';
```

## What to do next

After deploying and setting up your StarRocks cluster, you can then proceed to design tables that best work for your scenarios. See [Understand StarRocks table design](../table_design/Table_design.md) for detailed instructions on designing a table.
