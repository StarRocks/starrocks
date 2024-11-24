---
displayed_sidebar: docs
---

# Prepare deployment files

This topic describes how to prepare StarRocks deployment files.

Currently, the binary distribution packages StarRocks provides on [the StarRocks official website](https://www.starrocks.io/download/community) support deployments only on x86-based CPU. If you want to deploy StarRocks with the ARM-based CPU, you need to prepare the deployment files using the StarRocks Docker image.

## For x86-based CPU

From v3.1.14, v3.2.10, and v3.3.3, StarRocks binary distribution packages are named in the `StarRocks-{Version}-{OS}-{ARCH}.tar.gz` format, where `Version` is a number (for example, `3.3.3`) that indicates the version information of the binary distribution package, `OS` indicates the operating system (including `centos` and `ubuntu`), and `ARCH` indicates the CPU architecture (currently only `amd64` is available, which is equivalent to x86_64). Make sure that you have chosen the correct version of the package.

:::note

In versions earlier than v3.1.14, v3.2.10, and v3.3.3, StarRocks provides binary distribution packages named in the `StarRocks-{Version}.tar.gz` format.

:::

Follow these steps to prepare deployment files for the x86-based platform:

1. Obtain the StarRocks binary distribution package directly from the [Download StarRocks](https://www.starrocks.io/download/community) page or by running the following command in your terminal:

   ```Bash
   # Replace <version> with the version of StarRocks you want to download, for example, 3.3.3,
   # and replace <OS> with centos or ubuntu.
   wget https://releases.starrocks.io/starrocks/StarRocks-<version>-<OS>-amd64.tar.gz
   ```

2. Extract the files in the package.

   ```Bash
   # Replace <version> with the version of StarRocks you want to download, for example, 3.3.3,
   # and replace <OS> with centos or ubuntu.
   tar -xzvf StarRocks-<version>-<OS>-amd64.tar.gz
   ```

   The package includes the following directories and files:

   | **Directory/File**     | **Description**                              |
   | ---------------------- | -------------------------------------------- |
   | **apache_hdfs_broker** | The deployment directory of the Broker node. |
   | **fe**                 | The FE deployment directory.                 |
   | **be**                 | The BE deployment directory.                 |
   | **LICENSE.txt**        | The StarRocks license file.                  |
   | **NOTICE.txt**         | The StarRocks notice file.                   |

3. Dispatch the directory **fe** to all the FE instances and the directory **be** to all the BE or CN instances for [manual deployment](../deployment/deploy_manually.md).

## For ARM-based CPU

### Prerequisites

You must have [Docker Engine](https://docs.docker.com/engine/install/) (17.06.0 or later) installed on your machine.

### Procedures

From v3.1.14, v3.2.10, and v3.3.3, StarRocks provides Docker images in the `starrocks/{Component}-{OS}:{Version}` format, where `Component` indicates the component the image (including `fe`, `be`, and `cn`), `OS` indicates the operating system (including `centos` and `ubuntu`), and `Version` is the version number (for example, `3.3.3`). Docker will automatically identify your CPU architecture and pull the corresponding image. Make sure that you have chosen the correct version of the image.

:::note

In versions earlier than v3.1.14, v3.2.10, and v3.3.3, StarRocks provides Docker images in the repositories `starrocks/artifacts-ubuntu` and `starrocks/artifacts-centos7`.

:::

1. Download a StarRocks Docker image from [StarRocks Docker Hub](https://hub.docker.com/r/starrocks/artifacts-ubuntu/tags). You can choose a specific version based on the tag of the image.

   ```Bash
   # Replace <component> with the component you want to download, for example, fe,
   # replace <version> with the version of StarRocks you want to download, for example, 3.3.3,
   # and replace <OS> with centos or ubuntu.
   docker pull starrocks/<Component>-<OS>:<version>
   ```

2. Copy the StarRocks deployment files from the Docker image to your host machine by running the following command:

   ```Bash
   # Replace <component> with the component you want to download, for example, fe,
   # replace <version> with the version of StarRocks you want to download, for example, 3.3.3,
   # and replace <OS> with centos or ubuntu.
   docker run --rm starrocks/<Component>-<OS>:<version> \
       tar -cf - -C /release . | tar -xvf -
   ```

3. Dispatch the deployment files to the corresponding instances for [manual deployment](../deployment/deploy_manually.md).
