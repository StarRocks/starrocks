---
displayed_sidebar: "English"
---

# Prepare deployment files

This topic describes how to prepare StarRocks deployment files.

Currently, the binary distribution packages StarRocks provides on [the StarRocks official website](https://www.starrocks.io/download/community) support deployments only on x86-based CentOS 7.9. If you want to deploy StarRocks with the ARM architecture CPUs or on Ubuntu 22.04, you need to prepare the deployment files using the StarRocks Docker image.

## For x86-based CentOS 7.9

StarRocks binary distribution packages are named in the **StarRocks-version.tar.gz** format, where **version** is a number (for example, **2.5.2**) that indicates the version information of the binary distribution package. Make sure that you have chosen the correct version of the package.

Follow these steps to prepare deployment files for the x86-based CentOS 7.9 platform:

1. Obtain the StarRocks binary distribution package directly from the [Download StarRocks](https://www.starrocks.io/download/community) page or by running the following command in your terminal:

   ```Bash
   # Replace <version> with the version of StarRocks you want to download, for example, 2.5.2.
   wget https://releases.starrocks.io/starrocks/StarRocks-<version>.tar.gz
   ```

2. Extract the files in the package.

   ```Bash
   # Replace <version> with the version of StarRocks you have downloaded.
   tar -xzvf StarRocks-<version>.tar.gz
   ```

   The package includes the following directories and files:

   | **Directory/File**     | **Description**                                              |
   | ---------------------- | ------------------------------------------------------------ |
   | **apache_hdfs_broker** | The deployment directory of the Broker node. From StarRocks v2.5 onwards, you do not need to deploy Broker nodes in general scenarios. If you need to deploy Broker nodes in your StarRocks cluster, see [Deploy Broker node](../deployment/deploy_broker.md) for detailed instructions. |
   | **fe**                 | The FE deployment directory.                                 |
   | **be**                 | The BE deployment directory.                                 |
   | **LICENSE.txt**        | The StarRocks license file.                                  |
   | **NOTICE.txt**         | The StarRocks notice file.                                   |

3. Dispatch the directory **fe** to all the FE instances and the directory **be** to all the BE or CN instances for [manual deployment](../deployment/deploy_manually.md).

## For ARM-based CPU or Ubuntu 22.04

### Prerequisites

You must have [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 or later) installed on your machine.

### Procedures

1. Download a StarRocks Docker image from [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/artifacts-ubuntu/tags). You can choose a specific version based on the tag of the image.

   - If you use Ubuntu 22.04:

     ```Bash
     # Replace <image_tag> with the tag of the image that you want to download, for example, 2.5.4.
     docker pull starrocks/artifacts-ubuntu:<image_tag>
     ```

   - If you use ARM-based CentOS 7.9:

     ```Bash
     # Replace <image_tag> with the tag of the image that you want to download, for example, 2.5.4.
     docker pull starrocks/artifacts-centos7:<image_tag>
     ```

2. Copy the StarRocks deployment files from the Docker image to your host machine by running the following command:

   - If you use Ubuntu 22.04:

     ```Bash
     # Replace <image_tag> with the tag of the image that you have downloaded, for example, 2.5.4.
     docker run --rm starrocks/artifacts-ubuntu:<image_tag> \
         tar -cf - -C /release . | tar -xvf -
     ```

   - If you use ARM-based CentOS 7.9:

     ```Bash
     # Replace <image_tag> with the tag of the image that you have downloaded, for example, 2.5.4.
     docker run --rm starrocks/artifacts-centos7:<image_tag> \
         tar -cf - -C /release . | tar -xvf -
     ```

   The deployment files include the following directories:

   | **Directory**        | **Description**                                              |
   | -------------------- | ------------------------------------------------------------ |
   | **be_artifacts**     | This directory includes the BE or CN deployment directory **be**, StarRocks license file **LICENSE.txt**, and StarRocks notice file **NOTICE.txt**. |
   | **broker_artifacts** | This directory includes the Broker deployment directory **apache_hdfs_broker**. From StarRocks 2.5 onwards, you do not need to deploy Broker nodes in general scenarios. If you need to deploy Broker nodes in your StarRocks cluster, see [Deploy Broker](../deployment/deploy_broker.md) for detailed instructions. |
   | **fe_artifacts**     | This directory includes the FE deployment directory **fe**, StarRocks license file **LICENSE.txt**, and StarRocks notice file **NOTICE.txt**. |

3. Dispatch the directory **fe** to all the FE instances and the directory **be** to all the BE or CN instances for [manual deployment](../deployment/deploy_manually.md).
