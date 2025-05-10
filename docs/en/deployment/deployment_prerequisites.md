---
displayed_sidebar: docs
---

# Deployment prerequisites

This topic describes the hardware and software requirements that your servers must meet before deploying StarRocks. For recommended hardware specifications of your StarRocks cluster, see [Plan your StarRocks cluster](../deployment/plan_cluster.md).

## Hardware

### CPU

StarRocks relies on AVX2 instruction sets to fully unleash its vectorization capability. Therefore, in a production environment, we highly recommend you deploy StarRocks on machines with x86 architecture CPUs.

You can run the following command in your terminal to check if the CPUs on your machines support the AVX2 instruction sets:

```Bash
cat /proc/cpuinfo | grep avx2
```

### Memory

No specific requirement is imposed on memory kits used for StarRocks. See [Plan StarRocks cluster - CPU and Memory](../deployment/plan_cluster.md#cpu-and-memory) for the recommended memory size.

### Storage

StarRocks supports both HDD and SSD as storage medium.

If your applications require real-time data analytics, intensive data scans, or random disk access, we strongly recommend you use SSD storage.

If your applications involve [Primary Key tables](../table_design/table_types/primary_key_table.md) with the persistent index, you must use SSD storage.

### Network

We recommend that you use 10 Gigabit Ethernet networking to ensure stable data transmission across nodes within your StarRocks cluster.

## Operating system

StarRocks supports deployments on Red Hat Enterprise Linux 7.9, CentOS Linux 7.9 or Ubuntu Linux 22.04.

## Software

You must install JDK 17 on your servers to run StarRocks v3.5 and later versions.

> **CAUTION**
>
> StarRocks does not support JRE.
