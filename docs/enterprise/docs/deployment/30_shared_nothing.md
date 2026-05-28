# Shared-nothing cluster

import TimezoneError from '../_assets/commonMarkdown/_timezone.mdx'

These steps are performed when deploying a shared-nothing database cluster after [installing CelerData Manager and deploying the agents on each database node](./10_install_prep.md). If you are deploying a shared-data database cluster, please switch to the [shared-data](./30_shared_data.md) documentation.

## FE deployment

You can identify which nodes to deploy FEs on, the dropdown is populated with the node list where you just installed the **Supervisor** and **Agent** services. Make sure you choose directories on disks large enough, and consider placing the logs on a disk separate from data.

After clicking Next it takes a while, you can check progress by looking at the disk activity on the servers.

import FE_Setup from '../_assets/manager/FE_Setup.png';

<img src={FE_Setup} alt="FE Config" style={{width: 500}} />;

## BE deployment

Manager will choose all of the nodes for BEs. If you would like to prevent a BE from being deployed on one or more of the servers hosting FEs, then remove those nodes by clicking on the red `-` to the right of the BE instance.

Make sure to set the Install Path to a disk with space, and edit it for each of the BEs being deployed. In the screenshot, the Install Path is set to `/data/sr`

import BE_Setup from '../_assets/manager/BE_Setup.png';

<img src={BE_Setup} alt="BE Config" style={{width: 700}} />;

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
