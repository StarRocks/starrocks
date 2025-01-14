# Prepare for installation

1. **Obtain information of your StarRocks cluster.** 

- If you already have a StarRocks cluster, obtain the cluster architecture (for example, the number of FE and BE nodes in the cluster), IP addresses and ports of the nodes, and passwords to access the nodes.
- If you want to deploy a new StarRocks cluster, you also need to plan the cluster architecture, IP addresses and ports of nodes, and passwords. 

1. **Prepare the environment and dependencies.**

:::note
Currently, each CelerData Manager can manage only one StarRocks cluster. If you have multiple clusters and want to install multiple CelerData Managers on the same machine, you must configure an external network port for each CelerData Manager.
:::

- We recommend that all your machines run CentOS 7 or later.  
- StarRocks does not have strict requirements on hardware. It can run on machines of both low and high configurations. The recommended configuration for a test environment is 8 cores 32 GB (logical cores) or higher, and the recommended configuration for an online environment is 16 cores or higher. The CPU must support AVX2 instruction set because BE nodes require AVX2 for high performance.
  -  You can run the following command to check whether your CPU supports AVX2 instruction sets.

```bash
cat /proc/cpuinfo | grep avx2
```

- Configure external network ports for external services to access CelerData Manager. If you need to access CelerData Manager from a data center or a machine in the cloud, the recommended port number range is **8000** to **9000**.

1. **Enable** **SSH** **password-free login for all nodes.**

:::note
When you deploy CelerData Manager, you need to use SSH and Python to transfer files between CelerData Manager and StarRocks cluster nodes. Therefore, you must enable **SSH password-free login for all nodes.** If you encounter the `permission denied` issue, contact the machine configuration administrator.
:::

a. Generate an SSH key pair.

```bash
ssh-keygen -t rsa
```

b. Copy the public key in the key pair to all machines. Note that `fe1`, `be1`, `be2`, and `be3` are the IP address or hostname of the machines to be deployed.

:::tip
The IP address here is the internal network IP of the machine. SSH for internal communication within the machine must also be enabled.
:::

```bash
ssh-copy-id fe1
ssh-copy-id be1
ssh-copy-id be2
ssh-copy-id be3
```

1. **Install** **python-setuptools for all nodes.**

When you deploy CelerData Manager, you must install Python and **python-setuptools.**

StarRocks is compatible with Python 2 and Python 3. The default save path is **/usr/bin/python.**

```bash
 yum install -y python python-setuptools
```

1. **Install** **and deploy** **MySQL** **Server.**

CelerData Manager uses MySQL to manage machine-related information, including meta information of the monitoring module.

If you have installed MySQL server, you can skip this step. CelerData Manager will create a database to store related information. We recommend that you use a MySQL Server maintained by a site reliability engineer (SRE).

Multiple StarRocks clusters can use the same MySQL database. We recommend that you use different names to identify different clusters.

1. **Check the disk configuration of your machine.**

Check whether the information of all the disks mounted to your machine can be found in **/etc/fstab**. If not, the disks cannot be automatically mounted to your machine after a restart.
