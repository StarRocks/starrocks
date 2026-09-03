---
displayed_sidebar: docs
sidebar_position: 1
description: "Version naming is detailed in the versioning documentation."
---

# Version Release Guide

Version naming is detailed in the [versioning](../introduction/versioning.md) documentation. Read that page first to understand **major**, **minor**, and **patch** versioning.

## Release Plan
- Release a minor version approximately every four months.
- Maintain the three latest minor versions (**minor** is the second number in the dot separated version, for example, in 3.4.2 **4** is the minor version). 

  With a minor version released every four months, a minor version should be expected to be supported for at most one year.

- Release a patch version within 2-3 weeks for the minor version in maintenance. 

## Pull Request Type

Every pull request in StarRocks should be titled with a type, including **feature**, **enhancement**, and **bugfix**. 

### Feature

- Definition: A feature is a new capability or functionality that did not previously exist in the database. It adds new behavior or significantly extends the existing functionality.
- Examples:
  - Adding a new type of data structure (e.g., a new table type or index type).
  - Implementing a new query language feature (e.g., a new SQL function or operator).
  - Introducing a new API endpoint or interface for interacting with the database.

### Enhancement

- Definition: An enhancement is an improvement to an existing feature or functionality. It does not introduce entirely new behavior but makes existing features better, faster, or more efficient.
- Examples:
  - Optimizing the performance of a query execution plan.
  - Improving the user interface of the database management tool.
  - Enhancing the security features by adding more granular access controls.

### Bugfix

- Definition: A bugfix is a correction of an error or flaw in the existing code. It addresses issues that prevent the database from functioning correctly or as intended.
- Examples:
  - Fixing a crash that occurs under certain query conditions.
  - Correcting an incorrect result returned by a query.
  - Resolving a memory leak or resource management issue.

## Cherry-pick Rule

We define some status for a minor version to assist cherry-pick management. You can find the version status in the `.github/.status` file.

For example, at the time this document was published, StarRocks version 3.4 is in the `feature-freeze` state and version 3.3 is `bugfix-only`. To verify this:

```bash
git switch branch-3.3
cat .github/.status
```

```bash
bugfix-only
```

1. `open`: All types of pull requests can be merged, including feature, enhancement, and bugfix.
2. `feature-freeze`: Only enhancement and bugfix pull requests can be merged.
3. `bugfix-only`: Only bugfix pull requests can be merged.
4. `code-freeze`: No pull requests can be merged except critical CVE fixes.

The minor version status will change with some baseline triggers, as shown below, and it can also be changed in advance if necessary.

1. When the minor version branch is created, it becomes `open` and stays `open` until it is released. 
2. When the minor version is released, it becomes `feature-freeze`.
3. When the next minor version is released, the previous minor version becomes `bugfix-only`.
4. The minor version stays as `bugfix-only` until three more minor versions are released, and then becomes `code-freeze`.

### Example

- branch-5.1 is created, this branch is in the `open` state until it passes through release candidate and is publicly released.
- Once version 5.1 is publicly released it enters the `feature-freeze` state.
- Once version 5.2 is publicly released 5.1 switches to `bugfix-only`.
- When versions 5.1, 5.2, 5.3, and 5.4 are all released:
  - 5.4 is in the `feature-freeze` state
  - 5.3 is in the `bugfix-only` state
  - 5.2 is also in the `bugfix-only` state
  - 5.1 is in the `code-freeze` state

## JDK Support Policy

StarRocks FE, BE, and CN nodes depend on a JDK. The JDK versions that StarRocks requires and recommends follow the Java LTS calendar published by [Eclipse Adoptium](https://adoptium.net/support), not StarRocks version numbers. For the JDK versions of each StarRocks version, see [JDK version by StarRocks version](../deployment/preparation/environment_configurations.md#jdk-version-by-starrocks-version).

Each StarRocks minor version defines two JDK versions:

- **Minimum JDK**: the oldest Java LTS release that Adoptium still builds. The FE does not start on an older JDK. The BE and CN log an error on an older JDK, and Java-dependent features such as Java UDFs and JNI-based connectors are not supported on it.
- **Recommended JDK**: the next Java LTS release. It is the JDK that the official container images ship, and it becomes the minimum JDK at the next change. Starting on a JDK that is older than the recommended JDK prints a deprecation warning that names the recommended JDK.

Java LTS releases newer than the recommended JDK are not yet covered by this policy. They usually work but are not tested. Non-LTS Java releases are not supported.

Currently, the minimum JDK is JDK 17 and the recommended JDK is JDK 21.

### When the minimum JDK changes

- The minimum JDK changes when the current minimum reaches its end of availability on Adoptium. At that point, the previous recommended JDK becomes the minimum, and the next Java LTS release becomes the recommended JDK.
- The recommended JDK is published one full cycle before it becomes the minimum, so you get about two years of notice through this policy, the JDK matrix, and the startup warning.
- The change takes effect in the first minor version released after the date, and only if the previous minor version already printed the startup warning for the new minimum. Otherwise, the change moves to the next minor version.
- A minor version keeps its minimum and recommended JDK for its whole life. A patch version never raises the minimum JDK, so upgrading to a patch version never requires a JDK upgrade. Support for a newer JDK can be added to an existing minor version in a patch version.

### Schedule

| Effective from | Minimum JDK | Recommended JDK | Reason |
| --- | --- | --- | --- |
| Oct 2027 | JDK 21 | JDK 25 | Java 17 reaches end of availability |
| Dec 2029 | JDK 25 | JDK 29 | Java 21 reaches end of availability |
| Sep 2031 | JDK 29 | JDK 33 | Java 25 reaches end of availability |

The dates are the end-of-availability dates published by Adoptium, which Adoptium states as "at least". They are fixed: if Adoptium later extends the availability of a Java version, the schedule does not change. Java 29 and Java 33 assume that Adoptium continues to designate an LTS release every two years.

### Components that run in another JVM

The Spark Load DPP library, the Hive Bitmap UDF library, and the Broker run inside another system's JVM. They target Java 8 and follow the JDK requirements of that system, not this policy.
