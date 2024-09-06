---
displayed_sidebar: docs
sidebar_label: "Feature Support"
---

# Feature Support: Data Distribution

This document outlines the partitioning and bucketing features supported by StarRocks.

## Supported table types

- **Bucketing**

  Hash Bucketing is supported in all table types. Random Bucketing (from v3.1 onwards) is supported **only in Duplicate Key tables**.

- **Partitioning**

  Expression Partitioning (from v3.1 onwards), Range Partitioning, and List Partitioning (from v3.1 onwards) are supported in all table types.

## Bucketing

<table>
    <tr>
        <th>Feature</th>
        <th>Key point</th>
        <th>Support status</th>
        <th>Note</th>
    </tr>
    <tr>
        <td rowspan="2">Bucketing strategy</td>
        <td>Hash Bucketing</td>
        <td>Yes</td>
        <td></td>
    </tr>
    <tr>
        <td>Random Bucketing</td>
        <td>Yes (v3.1+)</td>
        <td>Random Bucketing is supported <strong>only in Duplicate Key tables</strong>.<br />From v3.2, StarRocks supports dynamically adjusting the number of tablets to create according to cluster information and the data size.</td>
    </tr>
    <tr>
        <td>Bucket Key data type</td>
        <td>Date, Integer, String</td>
        <td>Yes</td>
        <td></td>
    </tr>
    <tr>
        <td rowspan="2">Bucket number</td>
        <td>Automatically set the number of buckets</td>
        <td>Yes (v3.0+)</td>
        <td>Automatically determined by the number of BE nodes or the data volume of the largest historical partition.<br />The logic has been optimized separately for partitioned tables and non-partitioned tables in later versions.</td>
    </tr>
    <tr>
        <td>Dynamic increase of the Bucket number for Random Bucketing</td>
        <td>Yes (v3.2+)</td>
        <td></td>
    </tr>
</table>

## Partitioning

<table>
    <tr>
        <th>Feature</th>
        <th>Key point</th>
        <th>Support status</th>
        <th>Note</th>
    </tr>
    <tr>
        <td rowspan="3">Partitioning strategy</td>
        <td>Expression Partitioning</td>
        <td>Yes (v3.1+)</td>
        <td>
            <ul>
                <li>Including Partitioning based on a time function expression (since v3.0) and Partitioning based on the column expression (since v3.1)</li>
                <li>Supported time functions: date_trunc, time_slice</li>
            </ul>
        </td>
    </tr>
    <tr>
        <td>Range Partitioning</td>
        <td>Yes (v3.2+)</td>
        <td>Since v3.3.0, three specific time functions can be used for Partition Keys: from_unixtime, from_unixtime_ms, str2date, substr/substring.</td>
    </tr>
    <tr>
        <td>List Partitioning</td>
        <td>Yes (v3.1+)</td>
        <td></td>
    </tr>
    <tr>
        <td rowspan="2">Partition Key data type</td>
        <td>Date, Integer, Boolean</td>
        <td>Yes</td>
        <td></td>
    </tr>
    <tr>
        <td>String</td>
        <td>Yes</td>
        <td>
            <ul>
                <li>Only Expression Partitioning and List Partitioning support String-type Partition Key.</li>
                <li>Range Partitioning does not support String-type Partition Key. You need to use str2date to transform the column to date types.</li>
            </ul>
        </td>
    </tr>
</table>

###  Differences between partitioning strategies

<table>
    <tr>
        <th rowspan="2"></th>
        <th colspan="2">Expression Partitioning</th>
        <th rowspan="2">Range Partitioning</th>
        <th rowspan="2">List Partitioning</th>
    </tr>
    <tr>
        <th>Time function expression-based Partitioning</th>
        <th>Column expression-based Partitioning</th>
    </tr>
    <tr>
        <td>Data type</td>
        <td>Date (DATE/DATETIME)</td>
        <td>
                  <ul>
                    <li>String (except BINARY)</li>
                    <li>Date (DATE/DATETIME)</li>
                    <li>Integer and Boolean</li>
           </ul>
        </td>
        <td>
                  <ul>
                    <li>String (except BINARY) [1]</li>
                    <li>Date or timestamp [1]</li>
                    <li>Integer</li>
           </ul>
        </td>
        <td>
                  <ul>
                    <li>String (except BINARY)</li>
                    <li>Date (DATE/DATETIME)</li>
                    <li>Integer and Boolean</li>
           </ul>
        </td>
    </tr>
    <tr>
        <td>Support for multiple Partition Keys</td>
        <td>/ (Only supports one date-type Partition Key)</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>Support Null values for Partition Keys</td>
        <td>Yes</td>
        <td>/ [2]</td>
        <td>Yes</td>
        <td>/ [2]</td>
    </tr>
    <tr>
        <td>Manual creation of partitions before data loading</td>
        <td>/ [3]</td>
        <td>/ [3]</td>
        <td>
            <ul>
                <li>Yes if the partitions are manually created in batch</li>
                <li>No if the dynamic partitioning strategy is adopted</li>
            </ul>
        </td>
        <td>Yes</td>
    </tr>
    <tr>
        <td>Automatic creation of partitions while data loading</td>
        <td>Yes</td>
        <td>Yes</td>
        <td>/</td>
        <td>/</td>
    </tr>
</table>

:::note

- [1]\: You need to use from_unixtime, str2date or other time functions to transform the column to date types.
- [2]\: Null values will be supported in Partition Keys for List Partitioning from v3.3.3 onwards.
- [3]\: Partitions are automatically created.

:::

For detailed comparisons between List Partitioning and Expression Partitioning, refer to [Comparison between list partitioning and expression partitioning](list_partitioning.md).

