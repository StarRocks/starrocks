# Compile StarRocks with Docker

This topic describes how to compile StarRocks using Docker.

## Overview

StarRocks provides development environment images for both Ubuntu 22.04 and CentOS 7.9. With the image, you can launch a Docker container and compile StarRocks in the container.

### StarRocks version and DEV ENV image

Different branches of StarRocks correspond to different development environment images provided on [StarRocks Docker Hub](https://hub.docker.com/u/starrocks).

- For Ubuntu 22.04:

  | **Branch name** | **Image name**              |
  | --------------- | ----------------------------------- |
  | main            | starrocks/dev-env-ubuntu:latest     |
  | branch-3.1      | starrocks/dev-env-ubuntu:3.1-latest |
  | branch-3.0      | starrocks/dev-env-ubuntu:3.0-latest |
  | branch-2.5      | starrocks/dev-env-ubuntu:2.5-latest |

- For CentOS 7.9:

  | **Branch name** | **Image name**                       |
  | --------------- | ------------------------------------ |
  | main            | starrocks/dev-env-centos7:latest     |
  | branch-3.1      | starrocks/dev-env-centos7:3.1-latest |
  | branch-3.0      | starrocks/dev-env-centos7:3.0-latest |
  | branch-2.5      | starrocks/dev-env-centos7:2.5-latest |

## Prerequisites

Before compiling StarRocks, make sure the following requirements are satisfied:

- **Hardware**

  Your machine must have at least 8 GB RAM.

- **Software**

  - Your machine must be running on Ubuntu 22.04 or CentOS 7.9.
  - You must have Docker installed on your machine.

## Step 1: Download the image

Download the development environment image by running the following command:

```Bash
# Replace <image_name> with the name of the image that you want to download, 
# for example, `starrocks/dev-env-ubuntu:latest`.
# Make sure you have choose the correct image for your OS.
docker pull <image_name>
```

Docker automatically identifies the CPU architecture of your machine and pulls the corresponding image that suits your machine. The `linux/amd64` images are for the x86-based CPUs, and `linux/arm64` images are for the ARM-based CPUs.

## Step 2: Compile StarRocks in a Docker container

You can launch the development environment Docker container with or without the local host path mounted. We recommend you launch the container with the local host path mounted, so that you can avoid re-downloading the Java dependency during the next compilation, and you do not need to manually copy the binary files from the container to your local host.

- **Launch the container with the local host path mounted**:

  1. Clone the StarRocks source code to your local host.

     ```Bash
     git clone https://github.com/StarRocks/starrocks.git
     ```

  2. Launch the container.

     ```Bash
     # Replace <code_dir> with the parent directory of the StarRocks source code directory.
     # Replace <branch_name> with the name of the branch that corresponds to the image name.
     # Replace <image_name> with the name of the image that you downloaded.
     docker run -it -v <code_dir>/.m2:/root/.m2 \
         -v <code_dir>/starrocks:/root/starrocks \
         --name <branch_name> -d <image_name>
     ```

  3. Launch a bash shell inside the container you have launched.

     ```Bash
     # Replace <branch_name> with the name of the branch that corresponds to the image name.
     docker exec -it <branch_name> /bin/bash
     ```

  4. Compile StarRocks in the container.

     ```Bash
     cd /root/starrocks && ./build.sh
     ```

- **Launch the container without the local host path mounted**:

  1. Launch the container.

     ```Bash
     # Replace <branch_name> with the name of the branch that corresponds to the image name.
     # Replace <image_name> with the name of the image that you downloaded.
     docker run -it --name <branch_name> -d <image_name>
     ```

  2. Launch a bash shell inside the container.

     ```Bash
     # Replace <branch_name> with the name of the branch that corresponds to the image name.
     docker exec -it <branch_name> /bin/bash
     ```

  3. Clone the StarRocks source code to the container.

     ```Bash
     git clone https://github.com/StarRocks/starrocks.git
     ```

  4. Compile StarRocks in the container.

     ```Bash
     cd starrocks && ./build.sh
     ```

## Troubleshooting

Q: The StarRocks BE building fails, and the following error message has been returned:

```Bash
g++: fatal error: Killed signal terminated program cc1plus
compilation terminated.
```

What should I do?

A: This error message indicates a lack of memory in the Docker container. You need to allocate at least 8 GB of memory resources to the container.

