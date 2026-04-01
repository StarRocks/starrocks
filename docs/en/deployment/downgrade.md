---
displayed_sidebar: docs
---

# Downgrade StarRocks

This topic describes how to downgrade your StarRocks cluster.

If an exception occurs after you upgrade a StarRocks cluster, you can downgrade it to the earlier version to quickly recover the cluster.

## Overview

Review the information in this section before downgrading. Perform any recommended actions.

### Downgrade paths

- **For patch version downgrade**

  You can downgrade your StarRocks cluster across patch versions, for example, from v3.5.11 directly to v3.5.6.

- **For minor version downgrade**

  For compatibility and safety reasons, we strongly recommend you downgrade your StarRocks cluster **consecutively from one minor version to another**. For example, to downgrade a StarRocks v3.5 cluster to v3.2, you need to downgrade it in the following order: v3.5.x --> v3.4.x --> v3.3.x --> v3.2.x.

- **For major version downgrade**

  You can only downgrade your StarRocks v4.1 cluster to v4.0.5 and later versions.

  :::warning

  **Downgrade Notes**

  - After upgrading StarRocks to v4.1, DO NOT downgrade to any 4.0 version below 4.0.5.

    Due to internal changes in data layout introduced in version 4.1 (related to tablet splitting and distribution mechanisms), clusters upgraded to 4.1 may generate metadata and storage structures that are not fully compatible with earlier versions. As a result, downgrade from 4.1 is only supported to version 4.0.5 or later. Downgrading to versions prior to 4.0.5 is not supported. This limitation is due to backward compatibility constraints in how earlier versions interpret tablet layout and distribution metadata.

  :::

### Downgrade procedure

StarRocks' downgrade procedure is the reverse order of the [upgrade procedure](../deployment/upgrade.md#upgrade-procedure). Therefore, you need to **downgrade** **FEs** **first and then BEs and CNs**. Downgrading them in the wrong order may lead to incompatibility between FEs and BEs/CNs, and thereby cause the service to crash. For FE nodes, you must first downgrade all Follower FE nodes before downgrading the Leader FE node.

## Before you begin

During preparation, you must perform the compatibility configuration if you are up for a minor or major version downgrade. You also need to perform the downgrade availability test on one of the FEs or BEs before downgrading all nodes in the cluster.

### Perform compatibility configuration

If you want to downgrade your StarRocks cluster to an earlier minor or major version, you must perform the compatibility configuration. In addition to the universal compatibility configuration, detailed configurations vary depending on the version of the StarRocks cluster you downgrade from.

- **Universal compatibility configuration**

Before downgrading your StarRocks cluster, you must disable tablet clone. You can skip this step if you have disabled the balancer.

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "0");
ADMIN SET FRONTEND CONFIG ("disable_balance"="true");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="true");
```

After the downgrade, you can enable tablet clone again if the status of all BE nodes becomes `Alive`.

```SQL
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_scheduling_tablets" = "10000");
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "500");
ADMIN SET FRONTEND CONFIG ("disable_balance"="false");
ADMIN SET FRONTEND CONFIG ("disable_colocate_balance"="false");
```

