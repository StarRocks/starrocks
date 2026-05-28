# Shared-data cluster

import TimezoneError from '../_assets/commonMarkdown/_timezone.mdx'

These steps are performed when deploying a shared-data database cluster after [installing CelerData Manager and deploying the agents on each database node](./10_install_prep.md). If you are deploying a shared-nothing database cluster, please switch to the [shared-nothing](./30_shared_nothing.md) documentation.

## FE deployment

You can identify which nodes to deploy FEs on, the dropdown is populated with the node list where you just installed the **Supervisor** and **Agent** services. Make sure you choose directories on disks large enough, and consider placing the logs on a disk separate from data.

Storage Types Supported: AWS, HDFS, OSS, OBS, COS, MinIO

Select the appropriate storage type and provide the required parameters. Different storage types support different authentication methods.

Refer to the documentation for details: [StarRocks Storage Volume Parameters](../sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md#parameters)

After clicking Next it takes a while, you can check progress by looking at the disk activity on the servers.

import FE_Setup from '../_assets/manager/FE_Setup_shared_data.png';

<img src={FE_Setup} alt="FE Config" style={{width: 800}} />

## Warehouse management

Under the **Nodes** tab, you can view which compute nodes belong to which warehouse.
By default, all compute nodes (CN) are assigned to `default_warehouse`.

To manage warehouses, go to the **Warehouse** tab within the Nodes section.

import Warehouse_Setup from '../_assets/manager/Warehouse.png';

<img src={Warehouse_Setup} alt="Warehouse Config" style={{width: 400}} />

## Create a new warehouse

To create a new warehouse, set the name and select the CN nodes to be assigned to the new warehouse.

import Create_Warehouse from '../_assets/manager/create_warehouse.png';

<img src={Create_Warehouse} alt="Warehouse Config" style={{width: 600}} />

## Suspend or delete a warehouse

To suspend or delete a warehouse, clock the appropriate button.

import Suspend_Warehouse from '../_assets/manager/warehouse_suspend.png';

<img src={Suspend_Warehouse} alt="Warehouse Config" style={{width: 800}} />

## Reassign CNs

CNs can be reassigned to different warehouses under **Nodes management**.

## Brokers

Brokers may be needed in some cases when using HDFS or Cloud storage.

## Center Service

This is the service that collects logs and metrics for the diagnostics, and manages alerts.

You have to set these:

- Metrics storage path
- Metrics retention days

Regarding SMTP: If you are not using email for alerts you can skip the SMTP related settings.

## Complete

Save your password and token to log in to the Manager UI.

Examples:

```bash
Password for `root` is: abcdef-c1c1234-44567-a94c-242468101214

Token for Emergency Mode is: 123456-2468-5123-9f45-05cac6394318
```

## License

When the deployment completes you will be presented with licensing information. Copy the license request string and the number of required CPUs and contact CelerData support.

For instructions on licensing clusters using RESTful API, see [License Your CelerData Cluster using RESTful API](./40_license_cluster.md).
