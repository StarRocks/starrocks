---
displayed_sidebar: "English"
---

# Compile StarRocks on Ubuntu

This topic describes how to compile StarRocks on the Ubuntu operating system. StarRocks supports compilation on both X86_64 and aarch64 architectures.

## Prerequisite

### Install Dependencies

Run the following commands to install necessary dependencies:

```bash
sudo apt-get update
```

```
sudo apt-get install automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 -y
```

### Install Compiler

If you are using Ubuntu 22.04 or later, run the following command to install the tools and compilers:

```bash
sudo apt-get install cmake gcc g++ default-jdk -y
```

If you are using an Ubuntu version earlier than 22.04, run the following commands to check the versions of tools and compilers:

1. Check GCC/G++ versions:

   ```bash
   gcc --version
   g++ --version
   ```

   GCC/G++ versions must be 10.3 or later. If you are using earlier versions, [click here to install GCC/G++](https://gcc.gnu.org/releases.html).

2. Check JDK version:

   ```bash
   java --version
   ```

   OpenJDK version must be 8 or later. If you are using an earlier version, [click here to install OpenJDK](https://openjdk.org/install).

3. Check CMake version:

   ```bash
   cmake --version
   ```

   CMake version must be 3.20.1 or later. If you are using an earlier version, [click here to install CMake](https://cmake.org/download).

## Compile StarRocks

Run the following command to start the compilation:

```bash
./build.sh
```

The default compilation parallelism is equal to **CPU core count/4**. Assuming you have 32 CPU cores, the default parallelism is 8.

If you want to adjust the parallelism, you can specify the number of CPU cores to be used for compilation via `-j` in the command line.

The following example uses 24 CPU cores for compilation:

```bash
./build.sh -j 24
```

## FAQ

Q: Building `aws_cpp_sdk` fails on Ubuntu 20.04 with the error "Error: undefined reference to pthread_create". How can I resolve this?

A: This error occurs due to a lower version of CMake. Please upgrade CMake to version 3.20.1 or above.
