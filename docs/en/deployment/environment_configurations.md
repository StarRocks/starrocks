---
displayed_sidebar: "English"
---

# Check environment configurations

This topic lists all environment and system configuration items that you must check and set before deploying StarRocks. Setting these configuration items properly allows your StarRocks cluster to work with high availability and performance.

## Ports

StarRocks uses specific ports for different services. Check whether these ports are occupied on each instance if you have deployed other services on these instances.

### FE ports

On the instances used for the FE deployment, you need to check the following ports:

- `8030`: FE HTTP server port (`http_port`)
- `9020`: FE Thrift server port (`rpc_port`)
- `9030`: FE MySQL server port (`query_port`)
- `9010`: FE internal communication port (`edit_log_port`)

Run the following commands on the FE instances to check whether these ports are occupied:

```Bash
netstat -tunlp | grep 8030
netstat -tunlp | grep 9020
netstat -tunlp | grep 9030
netstat -tunlp | grep 9010
```

If any of the above ports are occupied, you must find alternatives and specify them later when you deploy FE nodes. For detailed instructions, see [Deploy StarRocks - Start the Leader FE node](../deployment/deploy_manually.md#step-1-start-the-leader-fe-node).

### BE ports

On the instances used for the BE deployment, you need to check the following ports:

- `9060`: BE Thrift server port (`be_port`)
- `8040`: BE HTTP server port (`be_http_port`)
- `9050`: BE heartbeat service port (`heartbeat_service_port`)
- `8060`: BE bRPC port (`brpc_port`)

Run the following commands on the BE instances to check whether these ports are occupied:

```Bash
netstat -tunlp | grep 9060
netstat -tunlp | grep 8040
netstat -tunlp | grep 9050
netstat -tunlp | grep 8060
```

If any of the above ports are occupied, you must find alternatives and specify them later when you deploy BE nodes. For detailed instructions, see [Deploy StarRocks - Start the BE service](../deployment/deploy_manually.md#step-2-start-the-be-service).

### CN ports

On the instances used for the CN deployment, you need to check the following ports:

- `9060`: CN Thrift server port (`be_port`)
- `8040`: CN HTTP server port (`be_http_port`)
- `9050`: CN heartbeat service port (`heartbeat_service_port`)
- `8060`: CN bRPC port (`brpc_port`)
- `9070`: An extra agent service port for CN (BE in v3.0) in a shared-data cluster (`starlet_port`)

Run the following commands on the CN instances to check whether these ports are occupied:

```Bash
netstat -tunlp | grep 9060
netstat -tunlp | grep 8040
netstat -tunlp | grep 9050
netstat -tunlp | grep 8060
netstat -tunlp | grep 9070
```

If any of the above ports are occupied, you must find alternatives and specify them later when you deploy CN nodes. For detailed instructions, see [Deploy StarRocks - Start the CN service](../deployment/deploy_manually.md#step-3-optional-start-the-cn-service).

## Hostnames

If you want to [enable FQDN access](../administration/enable_fqdn.md) for your StarRocks cluster, you must assign a hostname to each instance.

In the file **/etc/hosts** on each instance, you must specify the IP addresses and corresponding hostnames of all the other instances in the cluster.

> **CAUTION**
>
> All IP addresses in the file **/etc/hosts** must be unique.

## JDK configuration

StarRocks relies on the environment variable `JAVA_HOME` to locate the Java dependency on the instance.

Run the following command to check the environment variable `JAVA_HOME`:

```Bash
echo $JAVA_HOME
```

Follow these steps to set `JAVA_HOME`:

1. Set `JAVA_HOME` in the file **/etc/profile**:

   ```Bash
   sudo vi /etc/profile
   # Replace <path_to_JDK> with the path where JDK is installed.
   export JAVA_HOME=<path_to_JDK>
   export PATH=$PATH:$JAVA_HOME/bin
   ```

2. Bring the change into effect:

   ```Bash
   source /etc/profile
   ```

Run the following command to verify the change:

```Bash
java -version
```

## CPU scaling governor

This configuration item is **optional**. You can skip it if your CPU does not support the scaling governor.

The CPU scaling governor controls the CPU power mode. If your CPU supports it, we recommend you set it to `performance` for better CPU performance:

```Bash
echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
```

## Memory configurations

### Memory Overcommit

Memory Overcommit allows the operating system to overcommit memory resources to processes. We recommend you enable Memory Overcommit.

```Bash
# Modify the configuration file.
cat >> /etc/sysctl.conf << EOF
vm.overcommit_memory=1
EOF
# Bring the change into effect.
sysctl -p
```

### Transparent Huge Pages

Transparent Huge Pages are enabled by default. We recommend you disable this feature because it can interfere with the memory allocator, and thereby lead to a drop in performance.

```Bash
# Temporary change.
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
# Permanent change.
cat >> /etc/rc.d/rc.local << EOF
if test -f /sys/kernel/mm/transparent_hugepage/enabled; then
   echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
fi
if test -f /sys/kernel/mm/transparent_hugepage/defrag; then
   echo madvise > /sys/kernel/mm/transparent_hugepage/defrag
fi
EOF
chmod +x /etc/rc.d/rc.local
```

### Swap Space

We recommend you disable Swap Space.

Follow these steps to check and disable Swap Space:

1. Disable Swap Space.

   ```SQL
   swapoff /<path_to_swap_space>
   swapoff -a
   ```

2. Delete the Swap Space information from the configuration file **/etc/fstab**.

   ```Bash
   /<path_to_swap_space> swap swap defaults 0 0
   ```

3. Verify that Swap Space is disabled.

   ```Bash
   free -m
   ```

### Swappiness

We recommend you disable swappiness to eliminate its impact on performance.

```Bash
# Modify the configuration file.
cat >> /etc/sysctl.conf << EOF
vm.swappiness=0
EOF
# Bring the change into effect.
sysctl -p
```

## Storage configurations

We recommend that you choose your suitable scheduler algorithm in accordance with the storage medium you use.

You can run the following command to check the scheduler algorithm that you are using:

```Bash
cat /sys/block/${disk}/queue/scheduler
# For example, run cat /sys/block/vdb/queue/scheduler
```

We recommend you use the mq-deadline scheduler for SATA disks and the kyber scheduler algorithm for SSD and NVMe disks.

### SATA

The mq-deadline scheduler algorithm suits SATA disks.

```Bash
# Temporary change.
echo mq-deadline | sudo tee /sys/block/${disk}/queue/scheduler
# Permanent change.
cat >> /etc/rc.d/rc.local << EOF
echo mq-deadline | sudo tee /sys/block/${disk}/queue/scheduler
EOF
chmod +x /etc/rc.d/rc.local
```

### SSD and NVMe

- If your NVMe or SSD disks support the kyber scheduler algorithm:

   ```Bash
   # Temporary change.
   echo kyber | sudo tee /sys/block/${disk}/queue/scheduler
   # Permanent change.
   cat >> /etc/rc.d/rc.local << EOF
   echo kyber | sudo tee /sys/block/${disk}/queue/scheduler
   EOF
   chmod +x /etc/rc.d/rc.local
   ```

- If your NVMe or SSD disks support the none (or noop) scheduler.

   ```Bash
   # Temporary change.
   echo none | sudo tee /sys/block/vdb/queue/scheduler
   # Permanent change.
   cat >> /etc/rc.d/rc.local << EOF
   echo none | sudo tee /sys/block/${disk}/queue/scheduler
   EOF
   chmod +x /etc/rc.d/rc.local
   ```

## SELinux

We recommend you disable SELinux.

```Bash
# Temporary change.
setenforce 0
# Permanent change.
sed -i 's/SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
sed -i 's/SELINUXTYPE/#SELINUXTYPE/' /etc/selinux/config
```

## Firewall

Open the internal ports for FE nodes, BE nodes, and Broker if your firewall is enabled.

```Bash
systemctl stop firewalld.service
systemctl disable firewalld.service
```

## LANG variable

Run the following command to check and configure the LANG variable manually:

```Bash
# Modify the configuration file.
echo "export LANG=en_US.UTF8" >> /etc/profile
# Bring the change into effect.
source /etc/profile
```

## Time zone

Set this item in accordance with your actual time zone.

The following example sets the time zone to `/Asia/Shanghai`.

```Bash
cp -f /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
hwclock
```

## ulimit configurations

Problems can occur with StarRocks if the values of **max file descriptors** and **max user processes** are abnormally small. We recommend you enlarge these values.

```Bash
cat >> /etc/security/limits.conf << EOF
* soft nproc 65535
* hard nproc 65535
* soft nofile 655350
* hard nofile 655350
* soft stack unlimited
* hard stack unlimited
* hard memlock unlimited
* soft memlock unlimited
EOF

cat >> /etc/security/limits.d/20-nproc.conf << EOF 
*          soft    nproc     65535
root       soft    nproc     65535
EOF
```

## File system configuration

We recommend you use the ext4 or xfs journaling file system. You can run the following command to check the mount type:

```Bash
df -Th
```

## Network configuration

### tcp_abort_on_overflow

Allow the system to reset new connections if the system is currently overflowed with new connection attempts that the daemon(s) can not handle:

```Bash
# Modify the configuration file.
cat >> /etc/sysctl.conf << EOF
net.ipv4.tcp_abort_on_overflow=1
EOF
# Bring the change into effect.
sysctl -p
```

### somaxconn

Specify the maximum number of connection requests queued for any listening socket to `1024`:

```Bash
# Modify the configuration file.
cat >> /etc/sysctl.conf << EOF
net.core.somaxconn=1024
EOF
# Bring the change into effect.
sysctl -p
```

## NTP configuration

You must configure time synchronization between nodes within your StarRocks cluster to ensure linear consistency of transactions. You can either use the internet time service provided by pool.ntp.org, or use the NTP service built in an offline environment. For example, you can use the NTP service provided by your cloud service provider.

1. Check if the NTP time server or Chrony service exists.

   ```Bash
   rpm -qa | grep ntp
   systemctl status chrony
   ```

2. Install the NTP service if there is not one.

   ```Bash
   sudo yum install ntp ntpdate && \
   sudo systemctl start ntpd.service && \
   sudo systemctl enable ntpd.service
   ```

3. Check the NTP service.

   ```Bash
   systemctl list-unit-files | grep ntp
   ```

4. Check the connectivity and monitoring status of the NTP service.

   ```Bash
   netstat -tlunp | grep ntp
   ```

5. Check if your application is synchronized with the NTP server.

   ```Bash
   ntpstat
   ```

6. Check the state of all the configured NTP servers in your network.

   ```Bash
   ntpq -p
   ```

## High concurrency configurations

If your StarRocks cluster has a high load concurrency, we recommend you set the following configurations.

### max_map_count

Specify the maximum number of memory map areas a process may have as `262144`:

```bash
# Modify the configuration file.
cat >> /etc/sysctl.conf << EOF
vm.max_map_count = 262144
EOF
# Bring the change into effect.
sysctl -p
```

### Other

```Bash
echo 120000 > /proc/sys/kernel/threads-max
echo 200000 > /proc/sys/kernel/pid_max
```
