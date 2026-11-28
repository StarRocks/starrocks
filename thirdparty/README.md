Enable frame pointer
===================

When introducing thirdparty, make sure compiler option `-fno-omit-frame-pointer` is enabled. By default it's disabled, which makes profiling hard.  https://gcc.gnu.org/onlinedocs/gcc-10.3.0/gcc/Optimize-Options.html. The ovehead of it can be offset by observablity.


BPF Performance Tools.pdf Chapter2

> On x86_64 today, most software is compiled with gcc’s defaults, breaking frame pointer stack traces. Last time I studied the performance gain from frame pointer omission in our production environment, it was usually less than one percent, and it was often so close to zero that it was difficult to measure. Many microservices at Netflix are running with the frame pointer reenabled, as the performance wins found by CPU profiling outweigh the tiny loss of performance. 

## Lance native reader

Lance is an optional prebuilt dependency, installed like tenann:

```bash
./thirdparty/build-thirdparty.sh -j2 lance
```

This downloads a versioned archive, verifies its pinned archive checksum and the
library/header checksums in its manifest, and installs it under
`thirdparty/installed/lance`. StarRocks does not build Rust or install Cargo.
The package includes the C ABI header, shared library, relocatable CMake config,
source/toolchain manifest and native integration-test dataset.

The Rust SDK wrapper, locked dependencies, build images and release tests live in
[starrocks-lance-ffi](https://github.com/beinan/starrocks-lance-ffi).
StarRocks consumes the Rocky9-built x86_64/ARM64 packages, whose glibc 2.34
floor also supports Ubuntu 22.04+. CentOS7 is not supported. The independently
published Ubuntu packages use Ubuntu 24.04 and are not selected here. For other platforms, build the standalone repository separately and
provide its installation through `StarRocksLance_ROOT`.

SDK upgrades are explicit dependency updates: release and test the wrapper in its
own repository, then update the artifact URL and checksums here. The wrapper ABI
version is independent of the upstream Lance SDK version.
