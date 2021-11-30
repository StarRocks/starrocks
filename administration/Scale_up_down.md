# Scale up and down

## FE scaling

StarRocks has two types of FE nodes: Follower and Observer. Followers are involved in election voting and writing. Observers are only used to synchronize logs and extend read performance.

When scaling up and down the FE, note that

* The number of Follower FEs (including Master) must be odd, and it is recommended to deploy 3 of them to form a High Availability (HA) mode.
* When the FE is in high availability deployment (1 Master, 2 Follower), it is recommended to add Observer FEs for better read performance. * Typically one FE node can work with 10-20 BE nodes. It is recommended that the total number of FE nodes be less than 10. Three is sufficient in most cases.

### FE scaling up

After deploying the FE node and starting the service, expand the FE node by command.

~~~sql
alter system add follower "fe_host:edit_log_port";
alter system add observer "fe_host:edit_log_port";
~~~

### FE Scaling Down

Scaling down is similar to scaling up:

~~~sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
~~~

After the expansion and contraction, you can view the node information by `show proc '/frontends';`

## BE Scaling

After BE scaling, StarRocks will automatically perform load-balancing without affecting the overall performance.

### BE scaling up

* Run the command to scale up

~~~sql
alter system add backend 'be_host:be_heartbeat_service_port';
~~~

* Run the command to check the BE status

~~~sql
show proc '/backends';
~~~

### BE scaling down

There are two ways to scale down a BE –  `DROP` and `DECOMMISSION`.

`DROP` will delete the BE node immediately, and the lost duplicates will be made up by FE scheduling. `DECOMMISSION` will make sure the duplicates are made up first, and then drop the BE node. `DECOMMISSION` is a bit more friendly and is recommended for scaling down.

The commands of both methods are similar:

* `alter system decommission backend "be_host:be_heartbeat_service_port";`
* `alter system drop backend "be_host:be_heartbeat_service_port";`

Drop backend is a dangerous operation, so you need to confirm it twice before executing it

* `alter system drop backend "be_host:be_heartbeat_service_port";`

The status of FE and BE after scaling up can also be checked by [Cluster Status](...). /administration/Cluster_administration.md#Confirm cluster health status).
