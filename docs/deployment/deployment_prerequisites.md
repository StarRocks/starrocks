# Deployment prerequisites

This topic describes the hardware and software requirements that your servers must meet before deploying StarRocks. For recommended hardware specifications of your StarRocks cluster, see [Plan your StarRocks cluster](../deployment/plan_cluster.md).

## Hardware

### CPU

StarRocks relies on AVX2 instruction sets to fully unleash its vectorization capability. Therefore, in a production environment, we highly recommend you deploy StarRocks on machines with x86 architecture CPUs.

You can run the following command in your terminal to check if the CPUs on your machines support the AVX2 instruction sets:

```Bash
cat /proc/cpuinfo | grep avx2
```

> **NOTE**
>
> ARM architecture does not support SIMD instruction sets, and therefore is less competitive than x86 architecture in some scenarios. Therefore, we only recommend deploying StarRocks on ARM architecture in a development environment.

### Memory

No specific requirement is imposed on memory kits used for StarRocks. See [Plan StarRocks cluster - CPU and Memory](../deployment/plan_cluster.md#cpu-and-memory) for the recommended memory size.

### Storage

StarRocks supports both HDD and SSD as storage medium.

If your applications require real-time data analytics, intensive data scans, or random disk access, we strongly recommend you use SSD storage.

If your applications involve [Primary Key tables](../table_design/table_types/primary_key_table.md) with the persistent index, you must use SSD storage.

### Network

We recommend that you use 10 Gigabit Ethernet networking to ensure stable data transmission across nodes within your StarRocks cluster.

## Operating system

StarRocks supports deployments on CentOS 7.9 or Ubuntu 22.04.

## Software

You must install JDK 1.8.0 on your servers to run StarRocks.

> **NOTE**
>
> StarRocks does not support JRE.

Follow these steps to install JDK:

1. Navigate to the path for the JDK installation.
2. Download JDK by running the following command:

   ```Bash
   wget --no-check-certificate --no-cookies \
       --header "Cookie: oraclelicense=accept-securebackup-cookie"  \
       http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz
   ```
