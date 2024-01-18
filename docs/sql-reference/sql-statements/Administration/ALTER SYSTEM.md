# ALTER SYSTEM

## Description

This statement is used to operate nodes in a system. (Administrator only!)

## Syntax

1. Add nodes (Please add in this way if multi-tenant functionality is not used).

   ```sql
   ALTER SYSTEM ADD BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...]
   ALTER SYSTEM ADD COMPUTE NODE "host:heartbeat_service_port"[,"host:heartbeat_service_port"...]
   ```

   After BE nodes are successfully added to your StarRocks cluster, you can execute the [SHOW BACKENDS](./SHOW%20BACKENDS.md) statement to see the newly added nodes.
   After CN nodes are successfully added to your StarRocks cluster, you can execute the [SHOW COMPUTE NODES](./SHOW%20COMPUTE%20NODES.md) statement to see the newly added nodes.

2. Add idle nodes (Namely, add BACKEND that does not belong to any cluster).

   ```sql
   ALTER SYSTEM ADD FREE BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...]
   ```

3. Add nodes to a cluster.

   ```sql
   ALTER SYSTEM ADD BACKEND TO cluster_name "host:heartbeat_port"[,"host:heartbeat_port"...]
   ```

4. Delete nodes.

   ```sql
   ALTER SYSTEM DROP BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...]
   ALTER SYSTEM DROP COMPUTE NODES "host:heartbeat_service_port"[,"host:heartbeat_service_port"...]
   ```

5. Take nodes offline.

   ```sql
   ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...]
   ```

6. Add Broker.

   ```sql
   ALTER SYSTEM ADD BROKER broker_name "host:port"[,"host:port"...]
   ```

7. Reduce Broker.

   ```sql
   ALTER SYSTEM DROP BROKER broker_name "host:port"[,"host:port"...]
   ```

8. Delete all Brokers.

   ```sql
   ALTER SYSTEM DROP ALL BROKER broker_name
   ```

9. Set up a Load error hub for collecting and displaying import errors.

   ```sql
   ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES ("key" = "value"[, ...])
   ```

10. Create an image (metadata snapshot) file.

   ```sql
   ALTER SYSTEM CREATE IMAGE
   ```

   Executing this statement triggers the Leader FE to create a new image (metadata snapshot) file. It is an asynchronous operation. You can check the start time and end time of the operation by viewing the FE log file **fe.log**. "Triggering a new checkpoint manually..." indicates that the operation has started, and "finished save image..." indicates that the image is created.

Note:

1. host could be a host name and an ip address.

2. heartbeat_port is the heartbeat port of the node.

3. Adding and deleting nodes are synchronous operations. Without considering the existing data on nodes, these two operations delete nodes from the metadata directly. Please operate with caution.

4. Nodes decommission means safely removing the nodes. This operation is asynchronous. If successful, the nodes will be deleted from metadata. If not, they will not be taken offline.

5. Nodes decommission process can be cancelled manually. For more details, please refer to CANCEL DECOMMISSION.

6. Load error hub:

   Currently, only two types of Hub are supported: Mysql and Broker. Please specify "type" = "mysql" or "type" = "broker" in PROPERTIES. To delete the current load error hub, please set the type as null.

   - When using the Mysql type, import errors will be inserted into specified Mysql database table and can be viewed directly through show load warnings statement.

     Hub of Mysql type needs to specify the following parameters:

   ```plain text
   host: mysql host
   port: mysql port
   user: mysql user
   password: mysql password
   database: mysql database
   table: mysql table
   ```

   - When using the Broker type, import errors will be generated into a file and be written into a designated remote storage system through Broker. Please make sure that corresponding broker is already deployed in the remote system.

     Hub of Broker type needs to specify the following parameters:

   ```plain text
   broker: Name of broker
   path: Remote storage path 
   other properties: Other information required for accessing remote storage, e.g. Authentication information, etc. 
   ```

## Examples

1. Add a node.

   ```sql
   ALTER SYSTEM ADD BACKEND "host:port";
   ```

2. Add an idle node.

   ```sql
   ALTER SYSTEM ADD FREE BACKEND "host:port";
   ```

3. Delete two nodes.

   ```sql
   ALTER SYSTEM DROP BACKEND "host1:port", "host2:port";
   ```

4. Take two nodes offline.

   ```sql
   ALTER SYSTEM DECOMMISSION BACKEND "host1:port", "host2:port";
   ```

5. Add two Hdfs Broker.

   ```sql
   ALTER SYSTEM ADD BROKER hdfs "host1:port", "host2:port";
   ```

6. Add a load error hub with Mysql type.

   ```sql
   ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
   ("type"= "mysql",
   "host" = "192.168.1.17"
   "port" = "3306",
   "user" = "my_name",
   "password" = "my_passwd",
   "database" = "starrocks_load",
   "table" = "load_errors"
   );
   ```

7. Add a load error hub with Broker type.

   ```sql
   ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
   ("type"= "broker",
   "name" = "bos",
   "path" = "bos://backup-cmy/logs",
   "bos_endpoint" = "http://gz.bcebos.com",
   "bos_accesskey" = "069fc278xxxxxx24ddb522",
   "bos_secret_accesskey"="700adb0c6xxxxxx74d59eaa980a"
   );
   ```

8. Delete the current load error hub.

   ```sql
   ALTER SYSTEM SET LOAD ERRORS HUB PROPERTIES
   ("type"= "null");
   ```
