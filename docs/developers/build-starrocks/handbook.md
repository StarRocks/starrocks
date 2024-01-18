# How to build StarRocks

In general, you can build StarRocks by just executing

```
./build.sh
```

This command will check if all the thirdpary dependencies are ready at first. If all dependencies are ready, it will build StarRocks `Backend` and `Frontend`.

After this command executes succefully, the generated binary will be in `output` directory.

## build FE/BE separately

You don't need to build both FE and BE each time, you can build them separately.
For example, you can only build BE by
```
./build.sh --be
```

and, only build FE by
```
./build.sh --fe
```

# How to run unit test

Unit tests of BE and FE are separted. In general, you can run BE test by
```
./run-be-ut.sh
```

run FE test by 
```
./run-fe-ut.sh
```

## How to run BE UT in command line

Now, BE UT needs some dependency to run, and `./run-be-ut.sh` helps it. But it is not flexible enough. When you want to run UT in the command-line, you can execute

```
UDF_RUNTIME_DIR=./ STARROCKS_HOME=./ LD_LIBRARY_PATH=/usr/lib/jvm/java-18-openjdk-amd64/lib/server ./be/ut_build_ASAN/test/starrocks_test
```

StarRocks Backend UT is built on top of google-test, so you can pass filter to run some of the UT, For example, you want to test only MapColumn related tests, you can execute

```
UDF_RUNTIME_DIR=./ STARROCKS_HOME=./ LD_LIBRARY_PATH=/usr/lib/jvm/java-18-openjdk-amd64/lib/server ./be/ut_build_ASAN/test/starrocks_test --gtest_filter="*MapColumn*"
```


# Build options

## build with clang

You can build StarRocks by `clang` too

```
CC=clang CXX=clang++ ./build.sh --be
```

Then you can see the following similar message in the build message

```
-- compiler Clang version 14.0.0
```

## build with different linker

The default linker is slow, developer can specify different linker to speed up linking.
For example, you can use `lld`, the LLVM-based linker.

You need to install `lld` firstly.

```
sudo apt install lld
```

Then you set the environment variable STARROCKS_LINKER with the linker you want to use.
For example:

```
STARROCKS_LINKER=lld ./build.sh --be
```

## build different type

You can build StarRocks with different types with different BUILD_TYPE variable, the default BUILD_TYPE is `RELEASE`. For example, you can build StarRocks with `ASAN` type by
```
BUILD_TYPE=ASAN ./build.sh --be
```
