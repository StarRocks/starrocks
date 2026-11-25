---
sidebar_position: 60
displayed_sidebar: docs
description: "StarRocks 表设计常见问题：表创建、schema 变更、分区、分桶和索引配置故障排查。"
products: [oss, enterprise]
---

## 排查表设计问题

本文解答了一些关于表设计的常见问题。

## VARCHAR 类型的最大长度是多少，长度如何影响查询性能？

VARCHAR 类型的最大长度是 1048576 字节。VARCHAR 是变长数据类型，其数据大小基于实际长度，内存也不会根据定义的长度预先分配。因此，建表时为同样的数据指定不同的 VARCHAR 长度，对查询性能的影响很小。STRING 类型等同于 VARCHAR(65533)。