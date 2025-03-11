# CelerData Manager

## Introduction

CelerData Manager is a visualized database management and development tool. It provides the following functions to improve the O&M efficiency and cut the O&M costs of your StarRocks clusters:

- Install, deploy, scale, upgrade, and roll back clusters.
- Monitor metrics, send alerts, and diagnose and identify possible issues.

CelerData Manager also provides an easy-to-use SQL editor to manage queries, track TopN scans, and analyze query execution, helping you accelerate queries and simplify operations.

### Function highlights

#### Cluster lifecycle management

- Visualized cluster deployment
- Online cluster scale-out/in, visualized node addition and decommissioning
- One-click upgrade and rollback

#### Dynamic cluster monitoring and alerting

- Provides 200+ metrics to monitor cluster performance, queries, data ingestion, and compaction (data version merge), achieving real-time, visualized monitoring.
- Identifies TopN scans and load tasks.
- Visualizes data ingestion and schema changes.
- Users can customize alerts using email and Webhook. They can also track alert records.

#### SQL editor and easy query analysis

- Provides a user-friendly SQL editor for users to track historical queries.
- Provides visualized analysis of query execution.
- Provides a slow query list to help users quickly identify query performance bottlenecks.

### Basic concepts

Before you install CelerData Manager and StarRocks cluster, get familiar with the following concepts of a StarRocks cluster: 

A StarRocks cluster consists of two types of modules: core modules and system modules.

- Core modules (enclosed in the yellow box in the following figure)
  - **Frontend (FE):** is responsible for metadata management, client connection management, query planning, and query scheduling. 
  - **Backend (BE):** is responsible for data storage, query execution, compaction, and replica management.
  - **Broker**: an intermediate service between StarRocks and external HDFS/object storage services. Brokers are used for data loading and exporting. 
- System modules (modules other than the core modules)
  - **Web**: provides a visualized graphical interface for users.
  - **Center service**: pulls and summarizes information reported by Agents, and provides query services.  
  - **Agent**: a program for information collection. It collects information such as metrics.

For more information about StarRocks, see [StarRocks architecture](https://docs.starrocks.io/en-us/latest/introduction/Architecture.).

## User manual

### Cluster lifecycle management

#### Prepare for installation

1. **Obtain information of your StarRocks cluster.** 

   - If you already have a StarRocks cluster, obtain the cluster architecture (for example, the number of FE and BE nodes in the cluster), IP addresses and ports of the nodes, and passwords to access the nodes.
   - If you want to deploy a new StarRocks cluster, you also need to plan the cluster architecture, IP addresses and ports of nodes, and passwords. 

2. **Prepare the environment and dependencies.**

   > **NOTE**
   >
   > Currently, each CelerData Manager can manage only one StarRocks cluster. If you have multiple clusters and want to install multiple CelerData Managers on the same machine, you must configure an external network port for each CelerData Manager.

   - We recommend that all your machines run Red Hat Enterprise Linux 7.9 or later.  
   - StarRocks does not have strict requirements on hardware. It can run on machines of both low and high configurations. The recommended configuration for a test environment is 8 logical cores and 32 GB memory or higher, and the recommended configuration for an online environment is 16 cores or higher.

     > The CPU must support AVX2 instruction set because BE nodes require AVX2 for high performance.

     You can run the following command to check whether your CPU supports AVX2 instruction sets.

     ```Apache
     cat /proc/cpuinfo | grep avx2
     ```

   - Configure external network ports for external services to access CelerData Manager. If you need to access CelerData Manager from a data center or a machine in the cloud, the recommended port number range is **8000** to **9000**.

3. **Enable SSH password-free login for all nodes.**

   > **NOTE**
   >
   > When you deploy CelerData Manager, you need to use SSH and Python to transfer files between CelerData Manager and StarRocks cluster nodes. Therefore, you must enable **SSH password-free login for all nodes**. If you encounter the `permission denied` issue, contact the machine configuration administrator.

   a. Generate an SSH key pair.

      ```Plain
      ssh-keygen -t rsa
      ```

   b. Copy the public key in the key pair to all machines. Note that `fe1`, `be1`, `be2`, and `be3` are the IP address or hostname of the machines to be deployed.

      > The IP address here is the internal network IP of the machine. SSH for internal communication within the machine must also be enabled.

      ```SQL
      ssh-copy-id fe1
      ssh-copy-id be1
      ssh-copy-id be2
      ssh-copy-id be3
      ```

4. **Install python-setuptools for all nodes.**

   When you deploy CelerData Manager, you must install Python and **python-setuptools**.

   StarRocks is compatible with Python 2 and Python 3. The default save path is **/usr/bin/python.**

   ```SQL
   yum install -y python python-setuptools
   ```

5. **Install and deploy MySQL Server.**

   CelerData Manager uses MySQL to manage machine-related information, including meta information of the monitoring module.

   If you have installed MySQL server, you can skip this step. CelerData Manager will create a database to store related information. We recommend that you use a MySQL Server maintained by a site reliability engineer (SRE).

   Multiple StarRocks clusters can use the same MySQL database. We recommend that you use different names to identify different clusters.

6. **Check the disk configuration of your machine.**

   Check whether the information of all the disks mounted to your machine can be found in **/etc/fstab**. If not, the disks cannot be automatically mounted to your machine after a restart.

#### Install CelerData Manager

1. Obtain **CelerData-EE-x.x.x.tar.gz** of the required version from your sales manager and decompress this package.

   ```Apache
   tar -zxvf Celerdata-EE-x.x.x.tar.gz
   ```

2. Install the Web UI and Center service.

   Go to the decompression directory, run the **install_path** script to generate a web interface, and install CelerData Manager.

   **install_path** is the installation directory of CelerData Manager on your machine. You can use the default path. If you need to install multiple CelerData Managers on the same machine or need to customize the directory, you can modify the **installation directory** or **port** as needed.

   > "Install multiple CelerData Managers on the same machine" means you can deploy multiple CelerData Managers on the same machine to manage different clusters.

   ```Bash
   cd Celerdata-EE-x.x.x

   sh bin/install.sh -h
   ```

   Supported configurations in **install.sh:**

   ```Apache
   -[d install_path] install_path(default: /home/disk1/celerdata/Celerdata-manager-20200101)
   -[y python_bin_path] python_bin_path(default: /usr/bin/python)
   -[p admin_console_port] admin_console_port(default: 19321)
   -[s supervisor_http_port] supervisor_http_port(default: 19320)
   ```

   If you deploy multiple CelerData Managers on one machine, you must specify the installation path for each CelerData Manager. You can use cluster name or IP address to differentiate CelerData Managers.

   Add arguments `-d <directory name>` and `-p <port number>` to modify the file directory and port of CelerData Manager.

   ```bash

   sh bin/install.sh -d /home/disk1/Celerdata-manager \

      -p 19125  -s 19025

   ```

   The specified directory is generated after this command is successfully executed.

   The generated directory contains the following three files. This directory is the installation directory of **Center service**.

   ```Apache

   drwxrwxr-x 9  4096 Feb 17 17:37 center

   -rwxrwxr-x 1   366 Feb 17 17:37 centerctl.sh

   drwxrwxr-x 3  4096 Feb 17 17:37 temp_supervisor

   ```

   After the installation, you can access the web interface.

   - For local access, use `localhost:19321`.
   - For external access via an IP address, use `http://192.168.x.x:19321`.  `19321` is the `admin_console_port` specified for `-p`. Default port: 19321. If a success message is displayed but you still cannot access the web UI, check your network settings to make sure that the port is not blocked by a firewall.
   - If you need to disable the **Web** service and the **Supervisor** that manages the **Web** service (for example, the Supervisor port is occupied or other errors occur), or need to modify the **Web** port, you can run the **centerctl.sh** script:

   ```Bash
   cd Celerdata-manager-xxx
   ./centerctl.sh -i

   # Go to the Supervisor interface.
   help # View commands.
   status  # Check the current service and commands such as stop and shutdown.

   # You can also reinstall CelerData Manager.
   ./centerctl.sh daemon
   ```

#### Deploy a StarRocks cluster via Web

1. Access the Web interface and configure a MySQL database for storing the management, query, and alerting information of CelerData Manager. 

   > If you have multiple CelerData clusters, we strongly recommend that you configure different MySQL accounts for different clusters to prevent unexpected issues caused by incorrect configurations.

2. After the configuration is complete, click **Test Connection.** If the test is successful (**OK** is displayed at the top of the page), click **Next**.
3. Specify the nodes to deploy, and the installation directories of **Agent** and **Supervisor**. Enter the internal network IP addresses for **Host IP** and use the default values for other parameters.

   - **Host IP**: You can configure multiple IP addresses at a time. Separate multiple IP addresses with semicolons (;).
   - Supervisor is used to manage the start and stop of processes.
   - Agent is responsible for collecting statistical information of the machine.

All the installations are performed in the user environment and will not affect system environment.

   > **NOTE**
   >
   > - The system has two types of **Supervisors**: one is used to manage Agent, BE, FE, and Broker; the other is used to manage **Web** and **Center service**. 
   > - If you want to deploy Agent, FE, BE, and Broker on the same machine where **Web** and **Center service** are deployed, check whether Supervisor ports conflict with an existing port. If there is a conflict, perform the following operations: Modify the previously configured `bin/install.sh -s ${new_port}` to specify the Supervisor port required by CelerData Manager, and make sure that all the Supervisors and Agents that manage FE, BE, and broker use the default ports.  

4. Click **Next**. In the displayed dialog box, select **Deploy a new Cluster** or **Migrate from an existing Cluster**. 

  - If you have deployed FE and BE, but not CelerData Manager, click **Migrate from an existed Cluster**.
  - If this is your first-time deployment, that is, no FE/BE programs are running on your machine, click **Deploy a new Cluster**.

##### Migrate an existing cluster

> This step is required if you upgrade your cluster from StarRocks open-source to Enterprise Edition. If you want to install a new cluster, perform the steps in 1.2.2 "Install a new cluster."

1. **Obtain the information of the original cluster.**

   If you connect to StarRocks via the MySQL client, run the following SQL commands to view and confirm the information of the FE , BE, and broker.

   ```SQL
   show frontends;
   show backends;
   show broker;
   ```

   Pay special attention to the following information:

     - Quantity, IP address, and version of FE , BE, and broker
     - Information of leader, follower, and observer FEs

2. Disable the daemon (such as Supervisor) of the original StarRocks cluster and start it using a script. 

   > **IMPORTANT**
   >
   > If this step is not performed, the Supervisor of the new CelerData Manager will conflict with the old Supervisor, causing the installation to fail.

   Assume that the installation directories are all under `~/Celerdata`. If the actual directories are different from this, modify the directories.

   ```Bash
   #### Use a script to start BE.
   # Check whether BE and Supervisor are started.
   echo -e "\n==== BE ====" && ps aux | grep be && echo -e "\n==== supervisor ====" && ps aux | grep supervisor

   cd ~/Celerdata/be
   # Turn off Supervisor and start it using a script.
   ./control.sh stop && sleep 3 && bin/start_be.sh --daemon

   # Use the above echo command to check whether BE and Supervisor are started.

   # Check BE startup on your MySQL client.
   mysql> show backends;


   #### Use a script to start FE.
   echo -e "\n==== FE ====" && ps aux | grep fe && echo -e "\n==== supervisor ====" && ps aux | grep supervisor

   cd ~/Celerdata/fe 
   # Turn off Supervisor and start it using a script.
   ./control.sh stop && sleep 2 && bin/start_fe.sh --daemon

   # Use the echo command to check whether FE and Supervisor are started.

   # Modify the configuration file and run it again.
   sed -i 's/DATE = `date +%Y%m%d-%H%M%S`/DATE = "$(date +%Y%m%d-%H%M%S)"/g' conf/fe.conf

   # Check FE startup in your MySQL client.
   mysql> show frontends;


   #### Use a script to start broker.
   echo -e "\n==== broker ====" && ps aux | grep broker &&
   echo -e "\n==== supervisor ====" && ps aux | grep supervisor

   cd ~/Celerdata/apache_hdfs_broker
   ./control.sh stop && sleep 2 && bin/start_broker.sh --daemon

   # Check broker startup in MySQL.
   mysql> show broker;


   #### Check Supervisor again.
   ps aux | grep supervisor
   ```

3. Ensure that the cluster enters the "non-supervisor daemons" state before you continue with the following steps.
4. Fill in the FE, BE, and broker installation directories. 

   > **NOTE**
   >
   > - This operation reinstalls FE and BE, and the configuration files are obtained from the metadata of the original FE.
   > - FE meta and BE storage retain the original data path.
   > - LOG uses the path in the new directory.
   > - Udf, syslog, audit log, small_files, and plugin_dir use the paths in the new directory.

   When you migrate FE, BE, and broker, configure the upgrade paths respectively. (The paths and installation directories of the original instance are required) .

     - Path before upgrade: full path to FE, BE, and broker, such as `/home/Celerdata/fe`. 
     - Installation directory: the overall installation directory, such as `/home/Celerdata-manager-xxx`. 

   You can also perform the configurations in batches.

5. Click **Next.** On the page that is displayed, click **Migrate All** to perform automatic migration. You can also click **Migrate Next** to migrate the nodes one by one. 
6. Click **Next** to configure **Center service**. 

   **Center service** pulls information from the Agent, summarizes and stores the information, and provides monitoring and alarming services. **Mail service** is the mailbox that receives notifications, which can be left blank and configured later.

   Time zone errors may occur when you configure **Center service**. If this error occurs, refer to [UTC errors](#utc-error) for troubleshooting.

7. Click **Finish.** You will be automatically redirected to the web login page. The default account is **root** and the password is empty. The following page will be displayed after the login. 
8. Copy the code string in the figure and send it to the StarRocks technical support personnel, who will return a license string. After you enter the license string, click **OK** to use CelerData Manager. 

After this operation is complete, CelerData Manager is successfully installed and you can use the default user `root`. The initial password for `root` is generated during the install and is displayed. The password is also in the log:

```Bash
grep -r password manager/center/log/web/*
```

##### Install a new cluster

1. Install FE. In the **Configure FE Instance** dialog box, configure the following parameters:
   - **FE Followers**: We recommend that you configure 1 or 3 follower FEs. 
   - **FE Observers**: You can leave observer FEs unspecified. You can also add observer FEs when the query pressure increases.   
   - **Meta Dir**: Metadata directory of StarRocks. Similar to manual installation, we recommend that you specify a separate metadata directory. 
   - Use default values for the installation directory, log directory, and port numbers. 
2. Install BE. In the **BE Setup** dialog box, configure the following parameters:
   - **Port**: We recommend that you use the default value for **BE Port**, **Web Service Port**, **Heartbeat Port,** and **BRPC Port** if these ports are not occupied by other services. You can leave these ports unspecified to use the default values.
   - **Path:** 
      - **Install Path**: Binary file installation directory. If not filled in, a directory similar to `StarRocks-20250101` will be created under the current user's home directory. The date used in the directory is the installation date. You need to specify an absolute path if you want to install the BE node under a different directory.
      - **Log Directory**: The log directory of the BE node. It is recommended to use the default value. You need to specify an absolute path if you want to store the log files under a different directory.
      - **Storage Path**: The storage path of the data files on the BE node. We recommend that you specify a separate data directory. You can specify multiple storage directories separated by semicolons. You need to specify an absolute path if you want to store the data files under a different directory.

   ![img]()

 

3. Install broker. We recommend that you install a broker for all nodes. 

4. Install **Center service.**

**Center service** pulls information from the Agent, summarizes and stores the information, and provides monitoring and alarming services. **Mail service** is the mailbox that receives notifications, which can be left blank and configured later.

In the **Center Service Setup** dialog box, configure the following parameters:

- **Service port**: We recommend that you use the default value for **Center Service Port** if it is not occupied by other services. You can leave the port unspecified to use the default value.
- **Metrics storage path**: The director where the cluster metrics are stored. It is recommended to use the default value. You need to specify an absolute path if you want to store the metrics under a different directory.
- **Metrics retention days**: The time duration (in days) for which the manager retains the metrics.
- **Email serve**r: The SMTP server address.
- **Email port**: The SMTP server port.
- **Email user**: The SMTP username.
- **Email from**: The email address used to send alert emails.
- **Email password**: The authorization password obtained after enabling SMTP.

![img]()

Time zone errors may occur when you configure **Center service**, refer to [UTC errors](#utc-error) for solutions.

After this operation is complete, the StarRocks cluster is successfully installed and you can use the default user **root** and an empty password to log in to CelerData Manager (you can change the password and add an account by referring to the related operations in MySQL).

From v2.5 onwards, CelerData Manager will generate a temporary root user and a random password. In the last step of deployment, a prompt displays that the password is ****** and you need to record the password. If you did not obtain the password in time, you can query it in the log, which records the temporary password. The command is as follows:

```SQL
grep -r password manager/center/log/web/*
```

#### Enable SSL

You can skip this step if you do not need SSL configuration.

1. Add `ssl_cert` and `ssl_key` lines to `<celerdata-manager installation dir>/center/conf/web.conf`:

   ```Shell
   [web]
   port =
   session_secret =
   session_name =
   ssl_cert =
   ssl_key =
   ```

   - `ssl_key` is the absolute path of `PEM encoded certificate private key`.
   - `ssl_cert` is the absolute path of `PEM encoded certificate body`.

2. In the installation directory of CelerData Manager, run `./centerctl.sh restart web` to restart Web UI and run `./centerctl.sh status web` to check the status of Web UI. If the state displays RUNNING, the restart succeeds.
3. Access `https://mgr_host:port` in your browser. 

#### Upgrade/Rollback

##### Upgrade

Obtain the CelerData Manager installation package of the target version from your CelerData sales manager and then decompress it.

```Shell
$ tar -zxvf  Celerdata-EE-x.x.x.tar.gz
$ cd Celerdata-EE-x.x.x/file
$ ls -l
```

After decompression, you can see two installation packages **StarRocks-x.x.x.tar.gz** and **Manager-x.x.x.targ.z** in the **file** directory.

**x.x.x** is a 3-digit version number. The first digit is the major version. The second digit is a minor version, and the third digit is the patch version. Patch versions are  generally released every two to four weeks. You can read the [release notes](https://docs.starrocks.io/zh/releasenotes/release-3.4/) of each version at the official website. 

The files in the **file** directory are as follows. You must select packages in this directory when you perform an upgrade.

```Shell
file
   ├── Manager-3.3.9.tar.gz
   ├── STREAM
   ├── StarRocks-3.3.9-ee.tar.gz
   ├── iperf-3.1.3.tar.gz
   ├── openjdk-11.0.20.1_1-linux-x64.tar.gz
   ├── openjdk-8u322-b06-linux-x64.tar.gz
   ├── rg
   └── supervisor-4.2.0.tar.gz
```

###### Upgrade CelerData Manager

The upgrade procedure is upgrading CelerData Manager first and then upgrading StarRocks. Upgrading CelerData Manager does not affect cluster services.

1. In the upper-right corner of the home page, click the **root** drop-down list and click the upgrade button indicated by a version. 

   ![img]()

2. In the **Hint** dialog box, enter the path of **Manger-xxx.tar.gz** in the decompressed **file** folder. 
3. Confirm that all the files are successfully uploaded and click **Confirm** to perform an automatic upgrade.

4. After the upgrade is successful, you can check whether the version is correct in the **root** drop-down list.

   Make sure that version of CelerData Manager is the target version before you can continue with subsequent operations.

###### Upgrade the StarRocks cluster

Before you upgrade the StarRocks cluster, make sure that CelerData Manager has been upgraded.

1. Choose **Nodes** > **Version Management**.
2. In the **Version** page that appears, click the **Add** button to add a StarRocks version. 
3. Enter the path of **StarRocks-x.x.x.tar.gz** in the decompressed **file** folder.  
4. Confirm the path is correct and click **Confirm**. The system automatically installs the version (all the buttons in the **Operation** column become unclickable). 
5. After the new version is added, its state displays OFFLINE and the buttons in the **Operations** column become clickable.
6. Click the **Switch** button to perform the upgrade. 
7. After the first BE node is upgraded (100%), the system pauses for you to check. If no issues are found, you can click **Upgrade all** to upgrade all the other FE and BE nodes. You can also click **Upgrade next one** to upgrade the nodes one by one.
8. If the upgrade of the first node fails, you can click **Rollback all** or **Rollback next one**. If the upgrade fails and you want to resume services as quickly as possible, you can perform a rollback immediately. The cause of the failure is displayed in the edit box. If the issue persists, you can contact StarRocks technical support.
9. If you click **Upgrade all,** a button **Rollback all i**s displayed at the bottom of this page for you to suspend the upgrade.
10. After the upgrade succeeds, the upgrade page exits and you are redirected to the **Node** page where the target version is displayed and the start time of all FEs and BEs are the time of this upgrade.

##### Rollback

The rollback operations are similar to upgrade operations, which involve replacing **lib** and **bin** folders and restarting the cluster.

If you want to perform a rollback during or after an upgrade, select a historical version and switch to that version (the current version becomes unclickable).

Example of rollback during an upgrade:

1. Click **Version Management**. 
2. On the **Version** page, click **Add** to add a new version.
3. Specify the path of the upgrade file and click **Confirm**. The new version is added to the **Version** page.
4. Click the **Switch** button next to the target version. The current version is dimmed and not clickable. Other versions are clickable.
5. In the dialog box that appears, click **Confirm**.
6. After a BE is upgraded, you can click **Roll back previous one** or **Roll back all**. 

After the rollback is complete, check the version on the **Nodes** tab.

#### Scale in/out

##### BE scale-out

1. Choose **Nodes** > **Add Host.** Click **Add Host** to deploy a new Agent to manage the node. (Adding nodes requires re-signing the License.) 

   ![img]()

   After the node is added, it will be displayed in the **Add Host** section. 

2. In the **BE node** section, click **Scale Out** and enter the required information in the **BE Setup** dialog box.

   ![img]()

3. Click **Confirm** to complete the scale-out. Load balancing will be automatically triggered after the scale-out is complete. 

If you need to increase the license quota, contact StarRocks technical support.

##### BE scale-in

Find the BE node to be removed. In the **Operation** column, click **Decommission** to remove the node. The decommissioning process takes a few minutes. After data migration is complete, you can see that the number of tablets on the BE gradually drops to 0, and then click **Stop**. The node becomes an idle node, waiting to be recycled.

During the decommission, you can view the progress by checking the number of tablets. If you need to cancel the decommission, click **Cancel Decommission**.

##### FE scale-out

1. In the **Add Host** section of the **Nodes** page, click **Add Host** to deploy a new Agent to manage the node. (Adding nodes requires re-signing the License.) 
2. After the node is added, click **Scale Out** and enter the required information. Click **Confirm** to complete the scale-out.

![img]()

##### FE scale-in

Find the FE node to be removed. In the **Operation** column, click **Decommission** to remove the node. After the node is decommissioned, run `SHOW FRONTENDS;` on the **Editor** page to check whether the node exists.

## User privileges

### Root password

From v2.5 onwards, CelerData Manager will generate a temporary root user and a random password. In the last step of deployment, a prompt displays that the password is ****** and you need to record the password. If you did not obtain the password in time, you can query it in the log, which records the temporary password. The command is as follows:

```Bash
cd  celerdata-manager-20201102/center
grep -r password /log/web/*
```

### SQL editor

You can connect to CelerData through a MySQL client, or use the Web UI of CelerData Manager to create cluster uses and grant privileges to users. The following figure shows operations on the **Editor** page of the Web UI.

![img]()

### Change password

We recommend that you change the root password after installation and keep the password safe. The following code snippet shows how to change the password.

```SQL
--- Example
ALTER USER 'root' IDENTIFIED BY '123456';
-- Syntax
ALTER USER user_identity [auth_option];

-- Parameters
user_identity: in the format of 'user_name'@'host'

auth_option: {
IDENTIFIED BY 'auth_string'
IDENTIFIED WITH auth_plugin
IDENTIFIED  WITH auth_plugin BY 'auth_string'
IDENTIFIED WITH auth_plugin AS 'auth_string'
}

--- Example
ALTER USER 'jack' IDENTIFIED BY '123456';
```

**Parameter description:**

- **user_identity**

Consists of two parts, `user_name` and `host`, in the format of `username@'userhost'`. For the "host" part, you can use `%` for fuzzy match. If `host` is not specified, "%" is used by default, meaning that the user can connect from any host.

- **auth_option**

Specifies the authentication method. Currently, the following authentication methods are supported: mysql_native_password and authentication_ldap_simple.

### Create roles/users

- Create role

  You can grant privileges to a role and assign roles to a user. Users with the same role share the privileges granted to this role.

  Create a role using the following command:

  ```SQL
  --- Create role
  CREATE ROLE role1;
  --- Query the created roles
  SHOW ROLES;
  ```

- Create user

  ```SQL
  -- Syntax
  CREATE USER user_identity [auth_option] 
  [DEFAULT ROLE 'role_name'];

  -- Parameters
  user_identity:'user_name'@'host'

  auth_option: {
      IDENTIFIED BY 'auth_string'
      IDENTIFIED WITH auth_plugin
      IDENTIFIED WITH auth_plugin BY 'auth_string'
      IDENTIFIED WITH auth_plugin AS 'auth_string'
  }

  -- Example: Create a user and assign a default role to the user.
  CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';
  ```

Parameter description:

- **CREATE USER**

  Creates a CelerData user. In CelerData, a user_identity uniquely identifies a user.

- **user_identity**

  Consists of two parts, `user_name` and `host`, in the format of `username@'userhost'`. For the "host" part, you can use `%` for fuzzy match. If `host` is not specified, "%" is used by default, meaning that the user can connect from any host.

- **auth_option**

  Specifies the authentication method. Currently, the following authentication methods are supported: mysql_native_password and authentication_ldap_simple.

- **DEFAULT ROLE**

  If a role is specified, the privileges of the role will be automatically granted to the newly created user. If not specified, the user does not have any privileges by default. The specified role must already exist. You can refer to [CREATE ROLE](https://docs.starrocks.io/en-us/3.0/sql-reference/sql-statements/account-management/CREATE ROLE) for more information.

### Delete roles/users

You can delete a role/user using the following command:

```SQL
  -- Delete a role.
  DROP ROLE role1;
  -- Delete a user.
  DROP USER 'jack'@'192.%'
```

### Grant privileges

You can grant privileges to a user or a role using the following commands: 

```SQL
GRANT privilege_list ON db_name[.tbl_name] TO user_identity [ROLE role_name];

GRANT privilege_list ON RESOURCE resource_name TO user_identity [ROLE role_name];

-- Grant the USAGE privilege on 'spark_resource' to user 'jack'@'%'.
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';

-- Grant the USAGE_PRIV privilege on 'spark_resource' to role 'my_role'.
GRANT USAGE ON RESOURCE 'spark_resource' TO ROLE 'my_role';

-- Query privileges of all users.
SHOW ALL GRANTS; 

--- Query privileges of a specified user.
SHOW GRANTS FOR jack@'%';

-- Query privileges of the current user.
SHOW GRANTS;
```

**Parameter description:**

**privilege_list**

  The privileges that can be granted to a user or a role. If you want to grant multiple privileges at a time, separate the privileges with commas (`,`). The following privileges are supported:

- `NODE_PRIV`: the privilege to manage cluster nodes such as adding nodes and decommissioning nodes. Only the root user has this privilege. Do not grant this privilege to other users.
- `ADMIN_PRIV`: all privileges except `NODE_PRIV`.
- `GRANT_PRIV`: the privilege for performing operations such as creating users and roles, deleting users and roles, granting privileges, revoking privileges, and setting passwords.
- `SELECT_PRIV`: the privilege to read data from databases and tables.
- `LOAD_PRIV`: the privilege to load data into databases and tables.
- `ALTER_PRIV`: the privilege to change schemas of databases and tables.
- `CREATE_PRIV`: the privilege to create databases and tables.
- `DROP_PRIV`: the privilege to delete databases or tables.
- `USAGE_PRIV`: the privilege to use resources.

`ALL `and `READ_WRITE` in earlier versions will be converted to `SELECT_PRIV, LOAD_PRIV, ALTER_PRIV, CREATE_PRIV, DROP_PRIV`; `READ_ONLY `will be converted to `SELECT_PRIV`.

The preceding privileges can be classified into the following three categories:

- Node privilege: `NODE_PRIV`
- Database and table privilege: `SELECT_PRIV`, `LOAD_PRIV`, `ALTER_PRIV`, `CREATE_PRIV`, and `DROP_PRIV`
- Resource privilege: `USAGE_PRIV`

StarRocks implements a new role-based access control (RBAC) system from v3.0 in which the privileges are renamed and new privileges are addd. You can refer to [Privileges supported by StarRocks](https://docs.starrocks.io/en-us/3.0/administration/privilege_item).

**db_name [.tbl_name]**

The database and table name. This parameter supports the following three formats:

- `.`: indicates all databases and tables. If this format is specified, the global privilege is granted.
- `db.*`: indicates a specific database and all tables in this database.
- `db.tbl`: indicates a specific table in a specific database.

> Note: When you use the `db.*` or `db.tbl` format, you can specify a database or a table that does not exist.

**resource_name**

The resource name. This parameter supports the following two formats:

- `*`: indicates all the resources.
- `resource`: indicates a specific resource.

> Note: When you use the `resource` format, you can specify a resource that does not exist.

**user_identity**

This parameter contains two parts: `user_name` and `host`. `user_name` indicates the user name. `host` indicates the IP address of the user. You can leave `host` unspecified or you can specify a domain for `host`. If you leave `host` unspecified, `host` defaults to `%`, which means you can access from any host. If you specify a domain for `host`, it may take one minute for the privilege to take effect. The `user_identity` parameter must be created by the CREATE USER statement.

**role_name**

The role name.

### **Revoke privileges**

You can revoke privileges from specified users or roles.

```SQL
REVOKE privilege_list ON db_name[.tbl_name] FROM user_identity [ROLE role_name];

REVOKE privilege_list ON RESOURCE resource_name FROM user_identity [ROLE role_name];

-- Revoke a privilege on a database from a user.
REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';

-- Revoke user jack's privilege to use a resource.
REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';
```

**Parameter description:**

**user_identity**

The usage is the same as the preceding description. The `user_identity` parameter must be created by the CREATE USER statement.  If you specify a domain for `host`, it may take one minute for the privilege to take effect. You can also revoke a privilege from a specified ROLE, and the role must exist.

### Set user properties

You can set user properties, including resources allocated to users.

```SQL
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value'];
-- Modify the maximum number of connections of user jack to 1000.
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
-- Modify cpu_share of jack to 1000.
SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';
```

The user properties here mean the properties for users, not for user_identity. For example, if two users, 'jack'@'%' and 'jack'@'192.%', are created using CREATE USER, then the SET PROPERTY statement can only be used for user jack, not 'jack'@'%' or 'jack'@'192.%'.

**key:**

Properties for superuser:

- `max_user_connections`: maximum number of connections
- `resource.cpu_share`: CPU quota

Properties for common users:

- `quota.normal`: quotas for normal users
- `quota.high`: quotas for high-level users 
- `quota.low`: quotas for low-level users

## Overview

After logging in to CelerData Manager, you can view the status of your cluster on the **Dashboard** page, including **cluster basic information, Cluster overview, Query overview, and Alarm**.

### Cluster basic information

Displays **data information** and **node information**.

- **Data information** includes the total data volume, today's added data volume, and yesterday's added data volume.
- **Cluster node information**

  Leader FE IP and port (the port is the BE heartbeat port).

### Cluster overview

The **Cluster overview** section displays general information of the cluster.

It displays QPS, average query response time, 99 percentile response, number of database connections, and volume of loaded data. You can also specify a time interval in the upper right corner.

#### **Query QPS**

You can view the QPS to monitor cluster load.

**Average query response time**

By querying the average response time, you can observe the average query latency of the cluster in the current time period to determine whether the query latency meets expectation, and whether some SQL needs to be tuned.

Click **View more** to view the 50 quantile response time, 75 quantile response time, 90 quantile response time, 95 quantile response time, 99 quantile response time, and 999 quantile response time.

#### Number of database connections

You can view the number of connections in the current cluster. The statistics are not the number of connected users. Connections from the same account to the cluster are also counted.

#### Data ingestion volume

You can view the data loading frequency of the cluster. The default unit of the y-coordinate is KB/second.

Click **View More** at the bottom to view the number of load tasks initiated, number of loaded rows, and amount of loaded data.

### Query overview

Choose **Dashboard** > **Query overview** to view the query execution success rate, number of successful queries, and number of failed queries.

Click **View More** at the bottom to view **All Queries** and **Slow Queries**.

**All Queries** displays all the SQL statements that have been executed in the cluster, as well as the execution status, time consumption, and users who execute the SQL.

You can click a query ID to show SQL details, query plan, execution time, and execution details.

**Query plan** shows the result of the EXPLAIN statement.

**Query details** show the detailed profile of the query. If this tab is unclickable, query profile is not enabled. You can enable query profile using `set is_report_success = true` (for versions earlier than v2.5) or `set enable_profile = true` (for v2.5 and later) and later, and run EXPLAIN again.

### Alarm

> **IMPORTANT**
>
> To detect any exceptions and risks in your cluster in advance, you must configure alarms for your cluster. For more information, see the "Alarms and Diagnose" section.

In the Alarm area, you can view the number of alarms triggered. Alarms are classified into three types in descending order of severity: Fatal, Warning, and Info.

Click **View more** to go to the **Alarms** page.

You can view alarm records, configure alarm rules, block nodes from reporting alarms, and configure alarm notification recipients when an alarm is triggered.

## Manage clusters and nodes

### Manage clusters

#### Cluster performance

Click **Cluster** in the top navigation bar to display customer performance information, including CPU usage, memory usage, disk I/O usage, disk usage, free disk space, packet transmission rate, number of transmitted packets, packet receiving rate, and number of received packets.

You can adjust the slider, or adjust the time interval in the upper-right corner to observe this information at a specific time.

#### Query monitoring

Click the **Query monitor** tab to display the query information of the cluster, including Query QPS, Query response time (AVG), 50 Quantile, 75 quantile, 90 quatinle, 95 quantile, 99 quantile, and 999 quantile.

#### Data ingestion volume

Click the **Data Loaded** tab to display the query information of the cluster, including information such as the number of load tasks initiated, the number of rows loaded, and the size of the loaded data.

#### Compaction

Click the **Compaction** tab to display the query information of the cluster, including baseline merged data versions, cumulative merged data versions, baseline merged data volume, and cumulative merged data volume.

#### TopN scans

Click the **Scan TopN** tab to display the query information of the cluster, including the number of finished scans, the scanned data size, and the number of scanned rows.

#### TopN loads

Click the **Load TopN** tab to display the load information of the cluster, including the number of finished load tasks, the size of data loaded, and the number of rows loaded.

You can click a table to query the real-time loading of a single table. Use starrocks_audit_tbl_ as an example.

### Manage nodes

#### Machine information

1. Click **Nodes** in the top navigation bar to display the cluster information, including FE node information, BE node information, and Broker node information.
2. Click **Version Management** in the upper-right corner to switch cluster versions.
3. Click **Add** and enter the path of the target StarRocks version to add a version.
4. After the version is added, click **Switch**.

You can also start and stop a node, edit its configuration, or decommission the node.

#### Metrics

Click the **Metrics** tab to display the monitoring information of the current cluster.

You can select metrics from the metrics list to monitor the cluster.

## Manage databases

### View database information

#### Catalogs

1. Click **Catalogs** in the top navigation bar to display catalog information, including catalog name, type, and comments.
2. Click a catalog to view its detailed information, including database name, size, and comments.
3. Click a database (for example, `jd`) and the **Database detail** page is displayed, which contains three tabs: **Tables**, **Tasks**, and **Statistics**.
4. Click a table to view the table information, for example, `site_access`.

   > **NOTE**
   >
   > If a materialized view is built on this table, the materialized view will also be displayed when you click this table.

   Detailed information includes partition name, visible data version, partition status, partition key, partition range, bucketing column, number of buckets, number of replicas, and data size of the partition.

   - **Visible version**: the number of data versions of the partition. The number varies depending on the number of load tasks. More load tasks result in a larger value of this parameter.
   - **State**: The partition status must be **NORMAL**.

5. Click a partition (`p20221206`) to view the partition information.

   The lower level of **Partitions** is **Tablets**. This figure shows the number of tablets, data rows, data versions, and data size on a BE node.

   **Data group**: The total number of data versions in the output of the SHOW PROC command.

6. Check details of tablets on a BE mode. Use BE node `10410` as an example.

   This figure shows information such as the tablet ID, the BE node on which the tablet is located, the valid versions, the number of data rows, the data version, the data size, and the status of the tablet.

   - **Valid version**: The cumulative number of versions of the tablet. A large number of load tasks results in a large value of this metric.
   - **Data group**: The number of versions of the tablet. The higher the load frequency, the larger the value. The default limit is 1000. A value greater than 1000 will affect load performance.

#### Task

Use database `jd` as an example. On the **Tasks** tab, you can view detailed information of load tasks in this database, including **Kafka Import, Other Import**, **Export**, **Schema Change**, **Creating View**, and **Clone**.

-  **Kafka Import, Other Import**

  You can filter tasks by label name, table name, and task status. You can perform the following operations on the target task: resume tasks in the PAUSED state, stop a RUNNING task, and drop a task. In addition, you can view other task information.

- **Export**

  Displays task status, progress, task type, start time, end time, and details.

- **Schema Change**

  Displays table name, task execution status, progress, start time, end time, timeout duration, and details.

-  **Creating View**

  Displays materialized view name, base table name, creation status, progress, start time, end time, timeout duration, and details.

- **Clone**

  Displays information of replica cloning tasks in the cluster, including two types of tasks (running tasks and pending tasks). Details include tablet ID, database, table, partition, and start time.

#### Statistics

Click the **Statistics** tab to view information about tablets in the cluster, including tablets with inconsistent data and tablets with slow queries.

### Use database editor

Click **Editor** in the top navigation bar to write SQL. This eliminates the need to use the MySQL client to connect to CelerData. You can directly run SQL in the web UI.

- Write SQL

  ![img]()

- Select database

  Click the drop-down list in the left pane to select a database you want to query.

- Search for table

  You can enter a keyword in the search box to search for a table. In addition, you can click the **Table details** tab in the lower-right part to view the table structure and data in the table.

  ![img]()

- Tab

  You can click **+** in the top area to add your own tab and open a new editor page. This way, you can have a separate operation page.

Click **Saved tabs** to view all tabs and manage the tabs, including open, delete, search, and batch delete operations.

- Use the Query editor

  You can write SQL in the **Query editor** and then click **Run**. You can choose to execute a single query or execute SQL in batches. After execution, you can view the query history, query result, and table details.

  ![img]()

  - The **Query history** tab displays query ID, query start time, status, duration, and SQL.
  - The **Query result** tab displays query ID, execution time, execution status, number of returned rows, and total number of result rows.
  - The **Table details** tab displays column ID, column type, whether the column value can be nullable,  whether the table is a Primary Key table, the default value of the column, and comments. You can also query the table schema and view data in the table.

- Analyze

  This function is equivalent to `set is_report_success = true`, that is, enabling profile reporting to analyze the SQL.

- Clear: Clears the **Query editor** pane to write new SQL.

### Manage queries

Click **Queries** in the top navigation bar to view all the query records (a query that takes more than **5s** is regarded as a slow query).

#### All queries

The **All queries** tab displays the start time, ID, status, duration, query user, and SQL statement of a query. If you have enabled the query profile feature (`set enable_profile = true` for v2.5 and later and `set is_report_success = true` for versions earlier than v2.5), click the corresponding query ID to view the query execution plan and query profile.

- You can click a SQL statement to copy it.
- You can enter a keyword in the search box to filter queries.
- You can click the **All queries** drop-down list to filter queries.
- You can filter query records by time range.

The query records are displayed by page. At the bottom of the page, you can choose the number of entries to display on each page. You can view query records following the sequence of page numbers or select a target page number.

#### Slow queries

The **Slow queries** tab shows all the slow queries identified by the system. Similar to the **All queries** tab, you can filter queries using the search box, by execution status, or by specifying a time range.

## Alarms and Diagnose

### Alarms

#### Alarm records

Choose **Alarms** > **Alarm Record**. This tab displays historical alarm records. You can filter alarms based on time, alarm severity, and alarm status. You can also search for an alarm by entering a keyword in the search box.

#### Alarm rules

The **Alarm Rules** tab displays all the configured alarm rules. You can modify, delete, and disable an alarm metric on this page. You can also view the details of an alarm rule and historical alarms.

You can click **Create** in the upper-right corner to add an alarm rule.

- **Trigger period**: You can configure the time range during which the alarm is effective, for example, 08:00:00--18:00:00.
- **Alarm interval**: the alarm reporting interval. An alarm cannot be repetitively reported within the period specified by this parameter.
- **Nodes**: the node on which the alarm is triggered. For example, you can configure an FE node.
- **Metric:** The alarm metric can be searched.
- **Rule name:** Generally, the name is the same as the alarm metric. You can also customize the name.
- **Rule1**
  - Alarm interval: value + time unit, for example, 1 hour.
  - Trigger condition: The supported conditions are average value and value. The comparison operators are `>=`, `>`, `=`, `<`, `<=`. You need to set a value threshold. Example: Average value < 80%.
  - Alarm severity in ascending order
    - **Info:** Cluster load or other functions exceed the normal range and you need to pay attention to this.
    - **Warning**: Some functions of the cluster are unavailable. You need to pay attention and fix it.
    - **Fatal**: The cluster is unavailable. You need to check related information at the earliest time and communicate with the business team to identify issues.
  -  You can add an alarm rule by clicking the plus sign (+) to the right of each rule.
- **Remarks**: You can add a remark for the alarm rule to describe the meaning of the metric and the severity.

#### Block nodes

You can block alarms for specific nodes. This way, alarms related to this node will not be reported. This function avoids unnecessary alarm triggering when you perform node maintenance operations.

#### Alarm notification

Currently, CelerData supports alarm notifications via email, DingTalk Robot, Feishu Robot, and Webhook. You can choose any method that suits your needs. The following parts describe how to configure email and webhook.

##### Configure email

1. Click **root** > **settings** > **Email SMTP Settings** .
2. click **Edit**.
3. In the **SMTP server** field, type the hostname for the SMTP server address you want to use. For example, smtp.example.com.
4. For the **SMTP port**, type the number of the SMTP port to use.
5. Type in your valid SMTP server credentials in the **User** and **Password** fields.
6. In **FROM** , type the email address to send notifications from.
7. Choose the **Auth Type** of SMTP server
8. (**Optional**) Type in **To EMail** and Click Test to verify that the information for the email server configuration is correct.
9. Click **Save**.

###### Configure the SMTP server

You can click **Settings** under **root** to configure the mailbox and SMTP server.

![img]()

![img]()

###### Configure mailbox

On the **Notify management tab**, click **Create** in the upper-right corner. In the **Create** dialog box, configure email.

![img]()

##### Configure Webhook

On the **Webhooks** tab, click **Create** in the upper-right corner.

You need to develop an interface for receiving Webhook alarms from the server.

CelerData sends the following HTTP request to the configured URL:

```Plain
method:
POST

header:
x-starrocks-db-signature = [signature: hex_str(sha1(secret+post_body))]
content-type = [req_header.content-type]

body:
{
 "level":        "Alarm severity",
 "ruleName":     "Alarm rule name",
 "alarmMessage": "Alarm message",
 "startTime":    "Alarm start time",
}

Note
1. The result of x-starrocks-db-signature must be verified by the receiver. The calculation method is as follows:
Use sha1 to encode the string (Secret + Received post body) and convert it into a hex string.

Example for Golang:
hash := shal.New()
hash.Write ([]byte(secret))
hash.Write(body)
signBytes := hash.Sum(nil)
sign := fmt.Sprintf("&x", signBytes)

2. content-type can be application/json or application/x-www-form-urlencoded.
3. Secret
```

### Diagnose

#### Log

Choose **Diagnose** > **Log**. On the **Log** tab, you can view FE and BE logs. You can also search for a log in the search box or by specifying a time interval.

#### System Diagnose

You can click **Create System Diagnose** in the upper-right corner to collect the following information for troubleshooting:

- **Cluster basic info**: including host list, hardware information, and StarRocks version.
- **StarRocks Configurations**: including items in the `fe.conf` and `be.conf` configuration files and session variables.
- **StarRocks Log**: including FE and BE logs.
- **Hardware Test**: CPU, environment variables, Iperf network interface test results, maximum number of opened files (ulimit -n), configuration of `/proc/sys/vm/overcommit_memory`, and configuration of `/proc/sys/vm/swappiness`.
- **Slow queries**: slow queries within a specific period (4 days by default) and the profile.
- **System Metrics**: metrics related to memory, CPU, and io.util.
- **BE Memory Info**: See [Memory management](https://docs.starrocks.io/en-us/latest/administration/Memory_management).

#### Hardware Test

Hardware test will check the CPU, memory, maximum number of opened files (ulimit -n), configuration of `/proc/sys/vm/overcommit_memory`, configuration of `/proc/sys/vm/swappiness`, Iperf network interface test results, environment variables, and disk random I/O test results of the selected node.

## Uninstall CelerData Manager and StarRocks

1. Run the following commands in sequence on the directories where all nodes are located. 

   ```Bash
   cd  celerdata-manager-20201102/agent
   ./agentctl.sh stop all
   ./agentctl.sh shutdown
   ```

2. Run the following commands on the machine where CelerData Manager is deployed. 

   ```Bash
   cd  celerdata-manager-20201102/center
   ./centerctl.sh stop all
   ./centerctl.sh shutdown 
   ```

3. Delete (or back up) all the **celerdata-xxx** and **celerdata-manager-xxx** directories. If you use customized directories, stop all the processes managed by Supervisor and delete all the related directories.

## Appendix

### UTC error

If there is a UTC time zone error during the configuration of **Center service**, add **`export TZ = 'America/New_York';`** to the **~/.bashrc** file and run the file. This operation sets the environment variable **TZ** to `America/New_York` and adds this setting to system variables.

Before installing the **Web** service, confirm that the time zone is changed**:**

```Shell
[ycg@StarRocks-sandbox04 ~]# export TZ=America/New_York
[ycg@StarRocks-sandbox04 ~]# date
Thu Jan 16 08:03:17 EST 2025
```
