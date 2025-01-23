# Prepare for installation
import TimezoneError from '../_assets/commonMarkdown/_timezone.md

1. **Obtain information of your StarRocks cluster.** 

- If you already have a StarRocks cluster, obtain the cluster architecture (for example, the number of FE and BE nodes in the cluster), IP addresses and ports of the nodes, and passwords to access the nodes.
- If you want to deploy a new StarRocks cluster, you also need to plan the cluster architecture, IP addresses and ports of nodes, and passwords. 

1. **Prepare the environment and dependencies.**

> Currently, each CelerData Manager can manage only one StarRocks cluster. If you have multiple clusters and want to install multiple CelerData Managers on the same machine, you must configure an external network port for each CelerData Manager.

- We recommend that all your machines run Red Hat Enterprise Linux 7.9 or later.  
- StarRocks does not have strict requirements on hardware. It can run on machines of both low and high configurations. The recommended configuration for a test environment is 8 logical cores and 32 GB memory or higher, and the recommended configuration for an online environment is 16 cores or higher. 
  - The CPU must support AVX2 instruction set because BE nodes require AVX2 for high performance.
  -  You can run the following command to check whether your CPU supports AVX2 instruction sets.

```Apache
cat /proc/cpuinfo | grep avx2
```

- Configure external network ports for external services to access CelerData Manager. If you need to access CelerData Manager from a data center or a machine in the cloud, the recommended port number range is **8000** to **9000**.

1. **Enable SSH password-free login for all nodes.**

> When you deploy CelerData Manager, you need to use SSH and Python to transfer files between CelerData Manager and StarRocks cluster nodes. Therefore, you must enable **SSH password-free login for all nodes.** If you encounter the `permission denied` issue, contact the machine configuration administrator.

a. Generate an SSH key pair.

```Plain
ssh-keygen -t rsa
```

b. Copy the public key in the key pair to all machines. Note that `fe1`, `be1`, `be2`, and `be3` are the IP address or hostname of the machines to be deployed.

> Note: The IP address here is the internal network IP of the machine. SSH for internal communication within the machine must also be enabled.

```SQL
ssh-copy-id fe1
ssh-copy-id be1
ssh-copy-id be2
ssh-copy-id be3
```

1. **Install python-setuptools for all nodes.**

When you deploy CelerData Manager, you must install Python and **python-setuptools.**

StarRocks is compatible with Python 2 and Python 3. The default save path is `/usr/bin/python`.

```SQL
 yum install -y python python-setuptools
```

1. **Install and deploy MySQL Server.**

CelerData Manager uses MySQL to manage machine-related information, including meta information of the monitoring module.

If you have installed MySQL server, you can skip this step. CelerData Manager will create a database to store related information. We recommend that you use a MySQL Server maintained by a site reliability engineer (SRE).

Multiple StarRocks clusters can use the same MySQL database. We recommend that you use different names to identify different clusters.

1. **Check the disk configuration of your machine.**

Check whether the information of all the disks mounted to your machine can be found in `/etc/fstab`. If not, the disks cannot be automatically mounted to your machine after a restart.

## Install CelerData Manager

1. Obtain `CelerData-EE-x.x.x.tar.gz` of the required version from your sales manager and decompress this package.

```Apache
tar -zxvf CelerData-EE-x.x.x.tar.gz
```

1. Install the Web UI and Center service
   1.  Go to the decompression directory, run the **install_path** script to generate a web interface, and install CelerData Manager.
   2.  **install_path** is the installation directory of CelerData Manager on your machine. You can use the default path. If you need to install multiple CelerData Managers on the same machine or need to customize the directory, you can modify the **installation directory** or **port** as needed.
   3. "Install multiple CelerData Managers on the same machine" means you can deploy multiple CelerData Managers on the same machine to manage different clusters.

```Bash
cd CelerData-EE-x.x.x

sh bin/install.sh -h
```

Supported configurations in `install.sh`

```Apache
-[d install_path] install_path(default: /home/disk1/celerdata/CelerData-manager-20200101)
-[y python_bin_path] python_bin_path(default: /usr/bin/python)
-[p admin_console_port] admin_console_port(default: 19321)
-[s supervisor_http_port] supervisor_http_port(default: 19320)
```

If you deploy multiple CelerData Managers on one machine, you must specify the installation path for each CelerData Manager. You can use cluster name or IP address to differentiate CelerData Managers.

Add arguments `-d <directory name>` and `-p <port number>` to modify the file directory and port of CelerData Manager.

```Apache
sh bin/install.sh -d /home/disk1/CelerData-manager \
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

The example above used port 19125 (`-p 19125`) to differentiate that installation from an installation using the default port. If you have multiple installations on one server specify the custom port when connecting, or `-p 19325` if you are connecting to the default installation.

- For local access, use localhost:19321.
- For external access via an IP address, use `http://192.168.x.x:19321`.  `19321` is the `admin_console_port` specified for `-p`. Default port: 19321. If a success message is displayed, but you still cannot access the web UI, check your network settings to make sure that the port is not blocked by a firewall.
- If you need to disable the **Web** service and the **Supervisor** that manages the **Web** service (for example, the Supervisor port is occupied or other errors occur), or need to modify the **Web** port, you can run the `centerctl.sh` script:

```Bash
cd CelerData-manager-xxx
./centerctl.sh -i

# Go to the Supervisor interface.
help # View commands.
status  # Check the current service and commands such as stop and shutdown.

# You can also reinstall CelerData Manager.
./centerctl.sh daemon
```
