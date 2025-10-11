# Enhance Warehouse Availability with Compute Node Groups

From v4.0.0 onwards, StarRocks supports creating Compute Node Groups in warehouses.

By adding a new management layer between the warehouse and the Compute Node, StarRocks provides better isolation ability of the compute resources in the warehouse, thus enhancing the warehouse availability.

## Scenarios

### High availability with Multi-AZ deployment

Deploying the Compute Nodes in a warehouse across multiple availability zones will incur cross-AZ traffic costs. You can effectively reduce the cost by deploying the Compute Nodes of the same group in the same AZ. FE will then distribute query tasks among different Compute Node Groups through the scheduling policy, thus achieving the high availability of the warehouse deployed with Multi-AZ.

### Elastic scalability

You can scale the warehouse compute resources on the Compute Node Group granularity to achieve better elasticity under high-concurrency scenarios.

### Zero downtime rolling upgrade

You can upgrade or downgrade the warehouse by enabling a Compute Node Group as substitution for the main Compute Node Group, thereby achieving zero downtime during warehouse upgrade or downgrade.

## Usage

:::note

The following operations require [WAREHOUSE-related privileges](./manage_warehouses.md#privileges).

:::

### Create a Compute Node Group in a warehouse

:::note

A built-in Compute Node Group `_builtin_cngroup_0_` will be automatically created along with the warehouse creation.

:::

Syntax:

```SQL
ALTER WAREHOUSE <warehouse_name> 
ADD CNGROUP [IF NOT EXISTS] <cn_group_name> 
[COMMENT <comment>] 
[PROPERTIES ("key"="value", ...)]
```

Parameters:

- `warehouse_name`: The name of the warehouse in which you want to create the Compute Node Group.
- `cn_group_name`: The name of the Compute Node Group you want to create.
- `comment`: Optional. The comment of the Compute Node Group.

### Add a Compute Node to a Compute Node Group

Syntax:

```SQL
ALTER SYSTEM ADD COMPUTE NODE "<cn_host>:<cn_heartbeat_service_port>" 
INTO WAREHOUSE <warehouse_name> [CNGROUP <cn_group_name>]
```

Parameters:

- `cn_host`: The IP address or FQDN of the Compute Node.
- `cn_heartbeat_service_port`: The heartbeat service port of the Compute Node.
- `warehouse_name`: The name of the warehouse to which the Compute Node Group belongs.
- `cn_group_name`: The name of the Compute Node Group to which you want to add the Compute Node. You can omit this parameter ONLY when there is one Compute Node Group in the warehouse. Otherwise, an error will be returned.

### Enable or disable a Compute Node Group

:::note

A Compute Node Group is enabled by default after created.

:::

Syntax:

```SQL
ALTER WAREHOUSE <warehouse_name> { ENABLE | DISABLE } CNGROUP <cn_group_name>
```

Parameters:

- `warehouse_name`: The name of the warehouse to which the Compute Node Group belongs.
- `cn_group_name`: The name of the Compute Node Group you want to enable or disable.

### Alter Compute Node Group properties

Syntax:

```SQL
ALTER WAREHOUSE <warehouse_name> MODIFY CNGROUP <cn_group_name> SET ("key"="value", ...)
```

Parameters:

- `warehouse_name`: The name of the warehouse to which the Compute Node Group belongs.
- `cn_group_name`: The name of the Compute Node Group for which you want to alter the properties.

### Show Compute Node Groups from a warehouse

Syntax:

```SQL
SHOW CNGROUPS FROM WAREHOUSE <warehouse_name>
```

Parameters:

- `warehouse_name`: The name of the warehouse in which you want to show the Compute Node Groups.

### Show Compute Nodes from a Compute Node Group

Syntax:

```SQL
SHOW NODES FROM WAREHOUSE <warehouse_name> CNGROUP <cn_group_name>
```

Parameters:

- `warehouse_name`: The name of the warehouse to which the Compute Node Group belongs.
- `cn_group_name`: The name of the Compute Node Group in which you want to show the Compute Nodes.

### Drop a Compute Node from a Compute Node Group

Syntax:

```SQL
ALTER SYSTEM DROP COMPUTE NODE "<cn_host>:<cn_heartbeat_service_port>" [FROM WAREHOUSE <warehouse_name> [CNGROUP <cn_group_name>] ]
```

Parameters:

- `cn_host`: The IP address or FQDN of the Compute Node.
- `cn_heartbeat_service_port`: The heartbeat service port of the Compute Node.
- `warehouse_name`: The name of the warehouse to which the Compute Node Group belongs. If this parameter is not specified, `default_warehouse` is used.
- `cn_group_name`: The name of the Compute Node Group from which you want to drop the Compute Nodes. If this parameter is not specified, the builtin Compute Node Group `_builtin_cngroup_0_` is used.

### Drop a Compute Node Group

Syntax:

```SQL
ALTER WAREHOUSE <warehouse_name> DROP CNGROUP [IF EXISTS] <cn_group_name> [FORCE]
```

Parameters:

- `warehouse_name`: The name of the warehouse to which the Compute Node Group belongs.
- `cn_group_name`: The name of the Compute Node Group you want to drop.
- `FORCE`: Drops a Compute Node Group regardless of its status. By default, an enabled Compute Node Group or one with Compute Nodes in it is not allowed to be dropped. You need to specify the `FORCE` keyword to drop it forcibly.

## See also

For observability of Compute Node Groups, see [Monitoring Metrics for Warehouse Compute Node Groups](../../administration/management/monitoring/metrics-warehouse_cngroup.md).
