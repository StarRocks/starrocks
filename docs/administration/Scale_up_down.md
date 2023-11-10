# Scale in and out

This topic describes how to scale in and out the node of StarRocks.

## Scale FE in and out

StarRocks has two types of FE nodes: Follower and Observer. Followers are involved in election voting and writing. Observers are only used to synchronize logs and extend read performance.

> * The number of follower FEs (including leader) must be odd, and it is recommended to deploy 3 of them to form a High Availability (HA) mode.
> * When the FE is in high availability deployment (1 leader, 2 followers), it is recommended to add Observer FEs for better read performance. * Typically one FE node can work with 10-20 BE nodes. It is recommended that the total number of FE nodes be less than 10. Three is sufficient in most cases.

### Scale FE out

After deploying the FE node and starting the service, run the following command to scale FE out.

~~~sql
alter system add follower "fe_host:edit_log_port";
alter system add observer "fe_host:edit_log_port";
~~~

### Scale FE in

FE scale-in is similar to the scale-out. Run the following command to scale FE in.

~~~sql
alter system drop follower "fe_host:edit_log_port";
alter system drop observer "fe_host:edit_log_port";
~~~

After the expansion and contraction, you can view the node information by running `show proc '/frontends';`.

## Scale BE in and out

After BE is scaled in or out, StarRocks will automatically perform load-balancing without affecting the overall performance.

### Scale BE out

Run the following command to scale BE out.

~~~sql
alter system add backend 'be_host:be_heartbeat_service_port';
~~~

Run the following command to check the BE status.

~~~sql
show proc '/backends';
~~~

### Scale BE in

There are two ways to scale in a BE node –  `DROP` and `DECOMMISSION`.

`DROP` will delete the BE node immediately, and the lost duplicates will be made up by FE scheduling. `DECOMMISSION` will make sure the duplicates are made up first, and then drop the BE node. `DECOMMISSION` is a bit more friendly and is recommended for BE scale-in.

The commands of both methods are similar:

* `alter system decommission backend "be_host:be_heartbeat_service_port";`
* `alter system drop backend "be_host:be_heartbeat_service_port";`

Drop backend is a dangerous operation, so you need to confirm it twice before executing it

* `alter system drop backend "be_host:be_heartbeat_service_port";`
