# Installation

import TimezoneError from '../_assets/commonMarkdown/_timezone.mdx'

## Overview

These are the general steps to install CelerData Enterprise and deploy a StarRocks Server cluster:

- Servers
  - Prepare disks, open ports, add a user, configure authentication
  - Install software prerequisites
- Install CelerData Manager
- Configure the Manager
  - User DB configuration
  - Nodes Setup
- Deploy CelerData Server
  - FE deployment
  - BE or CN deployment
  - Brokers
  - Center Service
  - Apply your license

## Servers
In addition to the CelerData Manager server, you will need to prepare hardware for the new CelerData Server cluster, or gather the information for an existing StarRocks cluster that you are migrating to CelerData Server.

- If you already have a StarRocks cluster, obtain the cluster architecture (for example, the number of FE and BE nodes in the cluster), IP addresses and ports of the nodes, and passwords to access the nodes.
- If you want to deploy a new CelerData Server cluster, you also need to plan the cluster architecture, IP addresses and ports of nodes, and passwords. To plan a new cluster read [plan StarRocks cluster](./15_plan_cluster.md).

### Prepare the environment and dependencies

- We recommend that all your machines run Red Hat Enterprise Linux 7.9 or later.  
- CelerData Server does not have strict requirements on hardware. It can run on machines of both low and high configurations. The recommended configuration for a test environment is 8 logical cores and 32 GB memory or higher, and the recommended configuration for a production environment is 16 cores or higher.

  :::note
  If you are using x86-64 CPUs on the BE/CN servers, the CPUs must support the AVX2 instruction set for high performance. You can run the following command to check whether your x86-64 CPU supports AVX2 instruction sets.

  ```bash
  cat /proc/cpuinfo | grep avx2
  ```
  :::

- Configure external network ports for external services to access CelerData Manager. If you need to access CelerData Manager from a data center or a machine in the cloud, the recommended port number range is **8000** to **9000**.

  :::tip
  All of the default ports and how they are used are described under [networking](#networking)
  :::

### Application account

The CelerData Manager and CelerData Server database processes should be owned by a non-root user.

Create a non-root user on each of the servers (CelerData Manager, FE, and BE/CN servers):

:::note
Use the same username on each server, in the examples the username is `celerdata`.
:::

```bash
sudo useradd celerdata
sudo passwd celerdata
```

### Configure password-free SSH login

  :::note
  CelerData Manager uses SSH and Python to transfer files between the Manager server and the database nodes. Therefore, you must enable SSH password-free login for all nodes. If you encounter `permission denied` issues, check the SSH configuration.
  :::

Run these commands on the Manager server to:

- Switch to the non-root user (`celerdata` in the example)
- Generate an RSA key pair for the non-root user (`celerdata` in the example)
- Copy the public key to each of the FE and BE/CN servers

:::note
Do not use a passphrase, just hit Enter
:::

```bash
su - celerdata
ssh-keygen -t rsa
```

`ssh-keygen` generated a private and public key pair. By default the key pair is placed in the user's `.ssh/` directory. The public key (`id_rsa.pub`) contains the public half of the key pair, and should be placed in a file on each of the servers hosting FEs and BEs/CNs to allow the manager to install and configure StarRocks on those servers.

The `open-ssh` package includes a utility to copy your public key to the other servers. Run `man ssh-copy-id` for details on this command.

:::tip
If you are uncomfortable with configuring authentication contact your system administrator. 
:::

Alernatively, you can run an equivalent command from the Manager server that does the same job as `ssh-copy-id`:

```bash
su -c celerdata
cat ~/.ssh/id_rsa.pub | ssh celerdata@<hostname> \
'umask 0077; mkdir -p .ssh; cat >> .ssh/authorized_keys && \
echo "Key copied"'
```

The `ssh-copy-id` and the manual command shown above both perform these steps:

1. The public key `~/.ssh/id_rsa.pub` is read and passed (piped) to the `ssh` command.
2. You are prompted for the password for the non-root user on the target machine.
3. The command passed to `ssh` (everything between `umask` and `copied`) will be run on the target machine:
  - The permission mask is set so that only the user can read, write, or execute files created during the running of the command.
  - The directory `~/.ssh` is created if it does not exist.
  - The public key is added to the `authorized_keys` file in the `.ssh` directory
  - The message "Key copied" is printed if the commands are successful.

### Verify password-free SSH

`ssh` from the Manager server to each of the FE/BE/CN servers to verify password-free SSH:

Run from the Manager server (susbstituting each FE/BE/CN servername or IP address for `<hostname>`):

```bash
su - celerdata

ssh <hostname>
exit
```

## Disk

You may install the database cluster in the home directory of the non-root user, or you may have added a disk specifically for the database. The directory that is used for the database deployment on each server must be owned by the non-root user. Assign ownership of storage for the database to the non-root user.

For example, if your cluster will be deployed under `/data/` and the non-root user is `celerdata`, run this command:

```bash
sudo chown celerdata /data
```

### Check the disk configuration of your machine

  Check whether the information of all the disks mounted to your machine can be found in `/etc/fstab`. If not, add an entry to the `fstab` or see your system administrator.

## Networking

The default ports used by CelerData Manager and CelerData Server are:

### Common (all servers)
| <div style={{width: '200px'}}>Port name</div> | <div style={{width: '68px'}}>Default</div> | <div style={{width: '125px'}}>Direction</div> | Explanation | 
|-----------|--------------|-------------------------|-------------|
| SSH | 22 | All `<-->` All | Management processes communicate via SSH |

### BE and CN instances
| <div style={{width: '200px'}}>Port name</div> | <div style={{width: '68px'}}>Default</div> | <div style={{width: '125px'}}>Direction</div> | Explanation | 
|-----------|--------------|-------------------------|-------------|
| be_port | 9060 | FE `-->` BE/CN | Port of thrift server on BE/CN, receiving requests from FE |
| webserver_port | 8040 | BE/CN `<-->` BE/CN | Port of http server on BE/CN |
| heartbeat_service_port | 9050 | FE `-->` BE/CN | Heartbeat server level port (thrift) on BE/CN, receive heartbeat from FE |
| brpc_port | 8060 | FE `<-->` BE/CN | BRPC port on BE/CN for communication between BEs | BE/CN `<-->` BE/CN |
| starlet_port | 9070 | FE `-->` BE | Port for BE/CN heartbeat service in storage and calculation separation mode | (In the integrated storage and computing mode, BE also needs to open this port.) |

### FE instances
| <div style={{width: '200px'}}>Port name</div> | <div style={{width: '68px'}}>Default</div> | <div style={{width: '125px'}}>Direction</div> | Explanation | 
|-----------|--------------|-------------------------|-------------|
| http_port | 8030 | FE `<-->` FE | Port of http server on FE | User `<-->` FE |
| rpc_port | 9020 | BE/CN `-->` FE | Thrift server port on FE | FE `<-->` FE |
| query_port | 9030 | User `<-->` FE | Port of mysql server on FE |
| edit_log_port | 9010 | FE `<-->` FE | Port for communication between bdbje on FE |
| cloud_native_meta_port | 6090 | FE `<-->` BE/CN | Cloud Native metadata service listening port in storage and calculation separation mode | FE `<-->` FE |

### Broker instances
| <div style={{width: '200px'}}>Port name</div> | <div style={{width: '68px'}}>Default</div> | <div style={{width: '125px'}}>Direction</div> | Explanation | 
|-----------|--------------|-------------------------|-------------|
| broker_rpc_port | 8000 | FE `-->` Broker | Thrift server on Broker for receiving requests | | BE `-->` Broker |

### Manager
| <div style={{width: '200px'}}>Port name</div> | <div style={{width: '68px'}}>Default</div> | <div style={{width: '125px'}}>Direction</div> | Explanation | 
|-----------|--------------|-------------------------|-------------|
| admin_console_port | 19321 | Manager external | Nginx does port forwarding for external web ports |

### Agent service
| <div style={{width: '200px'}}>Port name</div> | <div style={{width: '68px'}}>Default</div> | <div style={{width: '125px'}}>Direction</div> | Explanation | 
|-----------|--------------|-------------------------|-------------|
| supervisor_http_port | 19320 | Internal | Supervisor management process | Center Service | 19319 | Supervisor management process |
| agent_port | 19323 | Agent `-->` Center | Agent and Center Service communicate, and users report monitoring information |

### Center service
| <div style={{width: '200px'}}>Port name</div> | <div style={{width: '68px'}}>Default</div> | <div style={{width: '125px'}}>Direction</div> | Explanation | 
|-----------|--------------|-------------------------|-------------|
| center_rpc_port | 19322 | Web `-->` Center | Communication ports for Web and Center Services |

During the deployment of the Supervisor and the Agent processes on the FE and BE/CN servers, and the deployment of the FEs and BEs/CNs you will see timeout messages if any ports are blocked by your firewall. After opening the ports retry the step that displayed the timeout.

### Install python-setuptools for all nodes

CelerData Manager requires Python and `python-setuptools`.

StarRocks is compatible with Python 2 and Python 3. Install Python and `python setuptools` on each server.

```bash
yum install -y python python-setuptools
```

You will need the path to `python` in later commands. Check the path:

```bash
which python
```

### Install and deploy MySQL Server

CelerData Manager uses MySQL to manage machine-related information, including meta information of the monitoring module.

If you have installed MySQL server, you can skip this step. CelerData Manager will create a database to store related information. We recommend that you use a MySQL Server maintained by a site reliability engineer (SRE).

Install MySQL on the CelerData Manager server. [Installation details](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/configuring_and_using_database_servers/assembly_using-mysql_configuring-and-using-database-servers#installing-mysql_assembly_using-mysql) on RHEL 9.0. 

```bash
sudo dnf install mysql-server
sudo systemctl start mysqld.service
sudo systemctl enable mysqld.service
```

After installing MySQL you should secure MySQL. Assign a good password, and save the password as you will need it later:

```bash
mysql_secure_installation
```

## Install CelerData Manager

Contact support for the CelerData Enterprise install file and copy the file to the server where you will install CelerData Manager.

Install the Manager software on the Manager server only. Use the non-root user that will also run StarRocks on each of the FEs and BEs/CNs.

```bash
tar -zxvf <distribution filename>.tar.gz
```

### Install the Web UI and Center service

  :::note
  Each CelerData Manager instance can manage only one database cluster. If you have multiple clusters and want to install multiple CelerData Managers on a single server, you must configure a separate installation directory, console port, and supervisor port for each CelerData Manager instance.
  :::

  ```bash
  cd CelerData-EE-x.x.x
  
  sh bin/install.sh -h
  ```

### Configuration notes

Supported configurations in `install.sh`

  ```bash
  -[d install_path] install_path(default: /home/disk1/celerdata/CelerData-manager-20200101)
  -[y python_bin_path] python_bin_path(default: /usr/bin/python)
  -[p admin_console_port] admin_console_port(default: 19321)
  -[s supervisor_http_port] supervisor_http_port(default: 19320)
  ```

  If you deploy multiple CelerData Managers on one machine, you must specify the installation path for each CelerData Manager. You can use cluster name or IP address to differentiate CelerData Manager instances.

  Add arguments `-d <directory name>` and `-p <port number>` to modify the file directory and port of CelerData Manager.

  ```bash
  sh bin/install.sh -d /data/CelerData-manager \
     -p 19125  -s 19025
  ```

  The specified directory is generated after this command is successfully executed.

  The generated directory contains the following three files. This directory is the installation directory of **Center service**.

  ```bash
  drwxrwxr-x 9  4096 Feb 17 17:37 center
  -rwxrwxr-x 1   366 Feb 17 17:37 centerctl.sh
  drwxrwxr-x 3  4096 Feb 17 17:37 temp_supervisor
  ```
  
  After the installation, you can access the web interface.

  The example above used port 19125 (`-p 19125`) to differentiate that installation from an installation using the default port. If you have multiple installations on one server specify the custom port when connecting, or `-p 19325` if you are connecting to the default installation.

  - For local access, use localhost:19321.
  - For external access via an IP address, use `http://192.168.x.x:19321`.  `19321` is the `admin_console_port` specified for `-p`. Default port: 19321. If a success message is displayed, but you still cannot access the web UI, check your network settings to make sure that the port is not blocked by a firewall.
  - If you need to disable the **Web** service and the **Supervisor** that manages the **Web** service (for example, the Supervisor port is occupied or other errors occur), or need to modify the **Web** port, you can run the `centerctl.sh` script:

  ```bash
  cd CelerData-manager-xxx
  ./centerctl.sh -i

  # Go to the Supervisor interface.
  help # View commands.
  status  # Check the current service and commands such as stop and shutdown.

  # You can also reinstall CelerData Manager.
  ./centerctl.sh daemon
  ```

## Configure CelerData Manager

Connect to http://localhost:19321 and you should see "User DB Configuration"

### User DB configuration

:::note
Multiple StarRocks clusters can use the same MySQL installation. We recommend that you use different database names to identify the information stored for each StarRocks cluster. When you fill out the MySQL configuration details, customize the database name used to store data for the StarRocks cluster that you will manage with this instance of CelerData Manager.
:::

Provide the MySQL configuration information. For example, if you installed MySQL on the same server as CelerData Manager:

|           |                                               |
|-----------|-----------------------------------------------|
| IP        | 127.0.0.1                                     |
| Port      | 3306                                          |
| Database  | manager_console                               |
| User name | root                                          |
| Password  | Password set with `mysql_secure_installation` |

After configuring the MySQL connection **Test connection** and look for **OK**.

import mySQLConfig from '../_assets/manager/MySQL.png';

<img src={mySQLConfig} alt="MySQL Config" style={{width: 500}} />;

### Nodes Setup

At this point, you are not deploying FEs or BEs/CNs: you are provisioning management software on each node that will host FEs and BEs/CNs. There are two processes that will be deployed on each server:

- **Supervisor** (`supervisord`) is used to manage the start and stop of processes.
- **Agent** (`agent_service`) is responsible for collecting statistical information of the machine.

import NodesSetup from '../_assets/manager/NodesSetup.png';

<img src={NodesSetup} alt="Nodes Config" style={{width: 500}} />;

### Install or Migrate

The next steps are to deploy the database cluster FEs and BEs or CNs. 

If you are deploying a Classic (shared-nothing) cluster follow the [shared-nothing](./30_shared_nothing.md) deployment steps.

If you are deploying an Elastic (shared-data) cluster follow the [shared-data](./30_shared_data.md) deployment steps.
