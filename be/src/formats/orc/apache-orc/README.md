# [Apache ORC](https://orc.apache.org/)

ORC is a self-describing type-aware columnar file format designed for
Hadoop workloads. It is optimized for large streaming reads, but with
integrated support for finding required rows quickly. Storing data in
a columnar format lets the reader read, decompress, and process only
the values that are required for the current query. Because ORC files
are type-aware, the writer chooses the most appropriate encoding for
the type and builds an internal index as the file is written.
Predicate pushdown uses those indexes to determine which stripes in a
file need to be read for a particular query and the row indexes can
narrow the search to a particular set of 10,000 rows. ORC supports the
complete set of types in Hive, including the complex types: structs,
lists, maps, and unions.

## ORC File Library

This project includes both a Java library and a C++ library for reading and writing the _Optimized Row Columnar_ (ORC) file format. The C++ and Java libraries are completely independent of each other and will each read all versions of ORC files. But the C++ library only writes the original (Hive 0.11) version of ORC files, and will be extended in the future.

Releases:
* Latest: <a href="http://orc.apache.org/releases">Apache ORC releases</a>
* Maven Central: <a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.orc%22">![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.orc/orc/badge.svg)</a>
* Downloads: <a href="http://orc.apache.org/downloads">Apache ORC downloads</a>

The current build status:
* Main branch <a href="https://travis-ci.com/apache/orc/branches">
![main build status](https://travis-ci.com/apache/orc.svg?branch=main)</a>
* <a href="https://travis-ci.com/github/apache/orc/pull_requests">Pull Requests</a>


Bug tracking: <a href="http://orc.apache.org/bugs">Apache Jira</a>


The subdirectories are:
* c++ - the c++ reader and writer
* cmake_modules - the cmake modules
* docker - docker scripts to build and test on various linuxes
* examples - various ORC example files that are used to test compatibility
* java - the java reader and writer
* proto - the protocol buffer definition for the ORC metadata
* site - the website and documentation
* snap - the script to build [snaps](https://snapcraft.io/) of the ORC tools
* tools - the c++ tools for reading and inspecting ORC files

### Building

* Install java 1.8 or higher
* Install maven 3 or higher
* Install cmake

To build a release version with debug information:
```shell
% mkdir build
% cd build
% cmake ..
% make package
% make test-out

```

To build a debug version:
```shell
% mkdir build
% cd build
% cmake .. -DCMAKE_BUILD_TYPE=DEBUG
% make package
% make test-out

```

To build a release version without debug information:
```shell
% mkdir build
% cd build
% cmake .. -DCMAKE_BUILD_TYPE=RELEASE
% make package
% make test-out

```

To build only the Java library:
```shell
% cd java
% mvn package

```

To build only the C++ library:
```shell
% mkdir build
% cd build
% cmake .. -DBUILD_JAVA=OFF
% make package
% make test-out

```
