# Deploy FE Cluster with High Availability

FE's high-availability clusters use a primary-secondary replication architecture to avoid single points of failure. FE uses the raft-like BDBJE protocol to complete leader selection, log replication, and failover. In FE clusters, instances are divided into two roles -- follower and observer. The follower is a voting member of the replication protocol, participating in the selection of the leader and submitting logs. Its general number is odd (2n+1). It takes majority (n+1) for confirmation and tolerates minority (n) failure. The observer is a non-voting member and is used to subscribe to replication logs asynchronously. The status of the observer lags behind the follower, similar to the leaner role in other replication protocols.

The FE cluster automatically selects the leader node from the followers. The leader node executes all state changes. A change can be initiated from a non-leader node, and then forwarded to the leader node for execution. The non-leader node records the LSN of the most recent change in the replication log. The read operation can be performed directly on the non-leader node, but it needs to wait until the state of the non-leader node gets synchronized with the LSN of the last operation. Observer nodes can increase the read load capacity of the FE cluster. Users with little urgency can read the observer nodes.

The clock difference between the FE nodes should not exceed 5s. Use the NTP protocol to calibrate the time.

A single FE node can only be deployed on one machine. The HTTP ports of all FE nodes need to be the same.

For cluster deployment, follow the following steps to add FE instances one by one.

Step 1: Distribute binary and configuration files (same as a single instance).

Step 2: Connect the MySQL client to the existing FE, and add the information of the new instance, including role, IP, port:

```sql
mysql> ALTER SYSTEM ADD FOLLOWER "host:port";
```

Or

```sql
mysql> ALTER SYSTEM ADD OBSERVER "host:port";
```

The host is the IP of the machine. If the machine has multiple IPs, select the IP in priority_networks. For example, `priority_networks=192.168.1.0/24` can be set to use the subnet `192.168.1.x` for communication. The port is `edit_log_port`, default to `9010`.

> Note: Due to security considerations, StarRocks' FE and BE can only listen to one IP for communication. If a machine has multiple network cards, StarRocks may not be able to automatically find the correct IP. For example, run the `ifconfig` command to get that `eth0 IP` is `192.168.1.1`, `docker0 : 172.17.0.1`. We can set the word network `192.168.1.0/24` to designate eth0 as the communication IP. Here we use [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) notation to specify the subnet range where the IP is located, so that it can be used on all BE and FE. `priority_networks` is written in both `fe.conf` and `be.conf`. This attribute indicates which IP to use when the FE or BE is started. The example is as follows: `priority_networks=10.1.3.0/24`.

If an error occurs, delete the FE by using the following command:

```sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
```

Step 3: FE nodes need to be interconnected in pairs to complete master selection, voting, log submission, and replication. When the FE node is first initiated, a node in the existing cluster needs to be designated as a helper. The helper node gets the configuration information of all the FE nodes in the cluster to establish a connection. Therefore, during initiation, specify the `--helper` parameter:

```shell
./bin/start_fe.sh --helper host:port --daemon
```

The host is the IP of the helper node. If there are multiple IPs, select the IP in `priority_networks`. The port is `edit_log_port`, default to `9010`.

There is no need to specify the `--helper` parameter for future starts. The FE stores other FEs’ configuration information in the local directory. To start directly:

```shell
./bin/start_fe.sh --daemon
```

Step 4: Check the cluster status and confirm that the deployment is successful:

```Plain Text
mysql> SHOW PROC '/frontends'\G

***1\. row***

Name: 172.26.x.x_9010_1584965098874

IP: 172.26.x.x

HostName: sr-test1

......

Role: FOLLOWER

IsMaster: true

......

Alive: true

......

***2\. row***

Name: 172.26.x.x_9010_1584965098874

IP: 172.26.x.x

HostName: sr-test2

......

Role: FOLLOWER

IsMaster: false

......

Alive: true

......

***3\. row***

Name: 172.26.x.x_9010_1584965098874

IP: 172.26.x.x

HostName: sr-test3

......

Role: FOLLOWER

IsMaster: false

......

Alive: true

......

3 rows in set (0.05 sec)
```

Alive is true, indicating the node is successfully added. In the above example, 172.26.x.x_9010_1584965098874 is the main FE node.
