---
sidebar_position: 60
displayed_sidebar: docs
description: "Frequently asked questions about StarRocks table design: troubleshooting table creation, schema changes, partitioning, bucketing, and index configuration."
products: [oss, enterprise]
---

## Troubleshooting Table Design

This topic provides answers to some frequently asked questions about table design.

## What is the maximum length of the VARCHAR type and how does the length affects query performance?

The maximum length of the VARCHAR type is 1048576 bytes. VARCHAR is a variable-length data type: its data size is based on the actual length of the data, and memory is not pre-allocated based on the declared length. Therefore, specifying a different VARCHAR length when you create a table has little impact on the query performance on the same data. The STRING type is equivalent to VARCHAR(65533).
