# Deploy FAQ

## **How to bind priority_networks parameters in fe.conf configuration file to a fixed IP?**

**Description of the problem：**

For  example, the client has 2 ip, namely, 192.168.108.23, 192.168.108.43. If you write data to 192.168.108.23/24, the system automatically identifies 43. If you write data to 192.168.108.23/32, it may cause error and the system automatically identifies 127.0.0.1.

**Solution:**

Drop 32 and only write the ip; or just write a relatively longer one, such as 28.

Note: (The error of 32 indicates the current version is old, but the new version has fixed the problem.)

## **be http_service didn't start correctly**

**Description of the problem:**

When installing "be", the system reports a startup error: Doris Be http service did not start correctly,exiting

**Solution:**

The problem is that 'be webservice' port is occupied. You can modify the port in 'be.conf' and restart it. If  the error message is reported repeatedly on the port which is not occupied after several modifications, check whether the program, such as yarn, is installed and modify the listening rule on listening port, or bypass the port selection range of 'be'.

## **Whether OS in SUSE 12SPS is supported?**

Can be supported. No problem is identified in testing.

ERROR 1064 (HY000): Could not initialize class org.apache.doris.rpc.BackendServiceProxy

Check whether 'jre' is used. If so, change 'jre' to 'jdk'. Oraclejdk version 1.8 plus is recommended.

## **[Enterprise deployment] An error occurs in node configuration during installation and deployment: Failed to Distribute files to node**

This error is due to the wrong version of setuptools. The following commands need to be executed in each computer because root permissions are needed:

```palin text
yum remove python-setuptools

rm /usr/lib/python2.7/site-packages/setuptool* -rf

wget https://bootstrap.pypa.io/ez_setup.py -O - | python
```

## **Can StarRocks temporarily modify the FE and BE configurations to make the modification take effect without restarting, for the production environment cannot be restarted unless it's necessary?**

Temporary modification of FE configuration:

SQL mode:

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

```sql
--Eg：
ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
```

Command mode:

```plain text
curl --location-trusted -u username:password http://ip:fe_http_port/api/_set_config?key=value
```

E.g.：

```plain text
curl --location-trusted -u root:root  http://192.168.110.101:8030/api/_set_config?enable_statistic_collect=true
```

Temporary modification of bE configuration:

Command mode:

```plain text
curl -XPOST -u username:password http://ip:be_http_port/api/update_config?key=value

Is the user not authorized to log on remotely:

CREATE USER 'test'@'%' IDENTIFIED BY '123456';
GRANT SELECT_PRIV ON . TO 'test'@'%';

Create user test and grant it permissions to log in again.
```

## **[Disk expansion problem] The disk space of BE is insufficient. After disks are added, load balancing fails on the data storage and an error occurs: Failed to get scan range, no queryable replica found in tablet: 11903**

**Description of the problem:**

A Flink import error occurs, indicating that the disk is insufficient. After disk expansion, load balancing of stored data cannot be performed, but the data is stored randomly.

**Solution:**

It's under repair. If the data is not important, the testing clients are advised to delete the disk directly. If the data is important or for the online clients, manual operation is recommended.

But one problem following the deletion of the disc is that an error message is displayed after the disk directory is changed:

`Failed to get scan range, no queryable replica found in tablet: 11903`，

The solution is to truncate table 11903.

## **During the cluster restart, a startup error of fe occors: Fe type:unknown ,is ready :false**

Confirm whether master is started, or try to restart the machine one by one.

## Error Installing Cluster：failed to get service info err

Check whether sshd is enabled on the machine. Check the status of sshd using /etc/init.d/sshd status.

## **Fail to start Be and the log reports an error: Fail to get master client from cache. host= port=0 code=THRIFT_RPC_ERROR**

Check whether be.conf port is occupied using `netstat  -anp  |grep  port`. If so, use another idle port and restart it.

## **When upgrading Manager in the expertise system, a prompt occurs: Failed to transport upgrade files to agent host. src:**…

Check whether the disk space is insufficient. During the cluster upgrade, the Manager distributes the binary file of the new version to each node. If the disk space of the deployment directory is insufficient, the file cannot be distributed, causing the aforementioned error.

## **The FE status of the newly added node is normal. However, an error occurs in the FE node logs displayed on the Diagnosis page of the Manager: "Failed to search log.**"

Manager obtains the path configuration of the newly deployed FE within 30 seconds by default. This problem occurs if the FE starts slowly or does not respond within 30 seconds due to other reasons. Check the log of Manager Web, and herein lies the log directory:`/starrocks-manager-xxx/center/log/webcenter/log/web/drms.INFO`, and search for the log to see whether there is the message: `Failed to update fe configurations`. If so, restart the corresponding FE service and the new path configuration will be obtained again.
