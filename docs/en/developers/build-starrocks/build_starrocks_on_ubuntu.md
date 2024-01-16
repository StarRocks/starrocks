---
displayed_sidebar: "English"
---

### Prerequisite

```
sudo apt-get update
```

```
sudo apt-get install automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 -y
```

### Compiler

If the ubuntu version >= 22.04, you can
```
sudo apt-get install cmake gcc g++ default-jdk -y
```

If the ubuntu version < 22.04.
Check the version of following tools and compilers

##### 1. GCC/G++

GCC/G++ version must be >= 10.3
```
gcc --version
g++ --version
```
Install GCC/G++(https://gcc.gnu.org/releases.html)

##### 2. JDK

OpenJDK version must be >= 8
```
java --version
```
Install OpenJdk(https://openjdk.org/install)

##### 3. CMake

cmake version must be >= 3.20.1

```
cmake --version
```
Install cmake(https://cmake.org/download)


### Improve the compile speed

The default compiling paralleilsim equals to the **CPU Cores / 4**.
If you want to improve the compile speed. If You can improve the paralleilsim.

1. Suppose you have 32 Cpu cores, the default paralleilsim is 8.

```
./build.sh
```

2. Suppose you have 32 Cpu cores, want to use 24 cores to compile.

```
./build.sh -j 24
```

### FAQ

1. Failed to build `aws_cpp_sdk` in the Ubuntu 20.04.
```
Error: undefined reference to pthread_create
```
The error comes from the lower CMake version; you can upgrade the CMake version to at least 3.20.1
