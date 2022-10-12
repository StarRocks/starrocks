# Enable FQDN access

This topic describes how to enable the access based on fully qualified domain name (FQDN). A FQDN is the **complete domain name** for a specific entity that can be accessed on the Internet. The FQDN consists of two parts: the hostname and the domain name.

StarRocks prior to version 2.4 supports access to each node via its IP address only. Even if an FQDN is used to add a node to a cluster, it is transformed to an IP address eventually. It causes a huge inconvenience for DBA because changing the IP addresses of certain nodes in a StarRocks cluster can lead to access failure to the nodes. In version 2.4, StarRocks decouples each node from its IP address. You can now manage nodes in StarRocks solely via their FQDNs.

## Prerequisites

To enable FQDN access for a StarRocks cluster, make sure the following requirements are satisfied.

- Each machine in the cluster must have a hostname.

- In the file **/etc/hosts** on each machine, you must specify the corresponding IP addresses and FQDNs of other machines in the cluster.

- You should not specify duplicate IP addresses in the file **/etc/hosts**.

## Set up a new cluster with FQDN access

To set up a new cluster with FQDN access, you should first install StarRocks version 2.4.0 or later. See [Deploy StarRocks](../quick_start/Deploy.md) for detailed instructions on how to install StarRocks.

In StarRocks version 2.4 or later, FE nodes in a new cluster are started via FQDNs by default.

Each BE node identifies itself with `BE Address` defined in the FE metadata. For example, if the `BE Address` defines a BE node with an FQDN, the BE node identifies itself with this FQDN.

Therefore, to start the nodes via FQDNs, you DO NOT need to specify the property `priority_networks` in the FE and BE configuration files **fe.conf** and **be.conf**. You can start each node as demonstrated in [Deploy StarRocks](../quick_start/Deploy.md) after requirements in [Prerequisites](#prerequisites) are met.

Alternatively, if you want to start the cluster with IP address access, you still have to specify the property `priority_networks` in the corresponding FE and BE configuration files before starting the nodes. Besides, because the IP address access is not adopted by default, you must start the FE nodes by running the following commands:

```Shell
sh bin/start_fe.sh --host_type IP --daemon
```

The property `--host_type` specifies the way of access that is used to start the node. The valid value includes `FQDN` and `IP`. You only need to specify this property ONCE when you start the node for the first time.

## Enable FQDN access in an existing cluster

To enable FQDN access in an existing cluster that was previously started via IP addresses, you must first **upgrade** StarRocks to version 2.4.0 or later.

### Enable FQDN access for FE nodes

You need to enable FQDN access for all the non-Leader Follower FE nodes before enabling that for the Leader FE node.

> **CAUTION**
>
> Make sure there are at least three Follower FE nodes in the cluster before enabling FQDN access for FE nodes.

#### Enable FQDN access for the non-Leader Follower FE nodes

1. Navigate to the deployment directory of the FE node, and run the following command to stop the FE node.

  ```Shell
  sh bin/stop_fe.sh --daemon
  ```

2. Execute the following statement via your MySQL client to check the `Alive` status of the FE node that you have stopped. Wait until the `Alive` status becomes `false`.

  ```SQL
  SHOW PROC '/frontends'\G
  ```

3. Execute the following statement to replace the IP address with FQDN.

  ```SQL
  ALTER SYSTEM MODIFY FRONTEND HOST "<fe_ip>" TO "<fe_hostname>";
  ```

4. Run the following command to start the FE node with FQDN access.

  ```Shell
  sh bin/start_fe.sh --host_type FQDN --daemon
  ```

  The property `--host_type` specifies the way of access that is used to start the node. The valid value includes `FQDN` and `IP`. You only need to specify this property ONCE when you restart the node after you modified the node.

5. Check the `Alive` status of the FE node. Wait until the `Alive` status becomes `true`.

  ```SQL
  SHOW PROC '/frontends'\G
  ```

6. Repeat the above steps to enable FQDN access for the other non-Leader Follower FE nodes one after another when the `Alive` status of the current FE node is `true`.

#### Enable FQDN access for the Leader FE node

After all non-Leader FE nodes have been modified and restarted successfully, you can now enable FQDN access for the Leader FE node.

> **NOTE**
>
> Before the Leader FE node is enabled with FQDN access, FQDNs used to add nodes to a cluster are still transformed to corresponding IP addresses. After a Leader FE node with FQDN access enabled is elected for the cluster, the FQDNs will not be transformed to IP addresses.

1. Navigate to the deployment directory of the Leader FE node, and run the following command to stop the Leader FE node.

  ```Shell
  sh bin/stop_fe.sh --daemon
  ```

2. Execute the following statement via your MySQL client to check if there is a new Leader FE node that has been elected for the cluster.

  ```SQL
  SHOW PROC '/frontends'\G
  ```

  Any FE node with status `Alive` and `isMaster` being `true` is a Leader FE that is running.

3. Execute the following statement to replace the IP address with FQDN.

  ```SQL
  ALTER SYSTEM MODIFY FRONTEND HOST "<fe_ip>" TO "<fe_hostname>";
  ```

4. Run the following command to start the FE node with FQDN access.

  ```Shell
  sh bin/start_fe.sh --host_type FQDN --daemon
  ```

  The property `--host_type` specifies the way of access that is used to start the node. The valid value includes `FQDN` and `IP`. You only need to specify this property ONCE when you restart the node after you modified the node.

5. Check the `Alive` status of the FE node.

  ```Plain
  SHOW PROC '/frontends'\G
  ```

  If the `Alive` status becomes `true`, the FE node is success modified and added to the cluster as a Follower FE node.

### Enable FQDN access for BE nodes

Execute the following statement via your MySQL client to replace the IP address with FQDN to enable FQDN access for the BE node.

```SQL
ALTER SYSTEM MODIFY BACKEND HOST "<be_ip>" TO "<be_hostname>";
```

> **NOTE**
>
> You DO NOT need to restart the BE node after FQDN access is enabled.

## FAQ

**Q: An error occurs when I enable FQDN access for an FE node: "required 1 replica. But none were active with this master". What should I do?**

A: Make sure there are at least three Follower FE nodes in the cluster before enabling FQDN access for FE nodes.

**Q: Can I add a new node by using IP address to a cluster with FQDN access enabled?**

A: Yes.
