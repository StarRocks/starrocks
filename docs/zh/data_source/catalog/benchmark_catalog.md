---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# Benchmark catalog

Benchmark catalog 是一种内置的 external catalog，可以为标准基准测试套件动态生成数据。借助它，您可以针对 TPC-H、TPC-DS 和 SSB schema 运行查询，而无需加载数据。

> **NOTE**
>
> 所有数据都是在查询时动态生成的，不会持久保存。请勿将此 catalog 用作基准测试源。

## 创建 Benchmark catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES ("key"="value", ...)
```

### 参数

#### `catalog_name`

Benchmark catalog 的名称。命名约定如下：

- 名称可以包含字母、数字 (0-9) 和下划线 (_)。必须以字母开头。
- 名称区分大小写，长度不能超过 1023 个字符。

#### `comment`

Benchmark catalog 的描述。此参数是可选的。

#### `PROPERTIES`

Benchmark catalog 的属性。`PROPERTIES` 必须包含以下参数：

| Parameter | Required | Default value | Description |
| --------- | -------- | ------------- | ----------- |
| type      | Yes      | None          | 数据源的类型。将值设置为 `benchmark`。 |
| scale     | No       | `1`           | 生成数据的 scale factor。该值必须大于 0。支持非整数值。 |

### 示例

```SQL
CREATE EXTERNAL CATALOG bench
PROPERTIES ("type" = "benchmark", "scale" = "1.0");
```

## 查询 benchmark 数据

Benchmark catalog 公开以下数据库：

- `tpcds`: TPC-DS schema。
- `tpch`: TPC-H schema。
- `ssb`: Star Schema Benchmark schema。

以下示例展示了如何切换到 catalog 并查询 SSB schema：

```SQL
SET CATALOG bench;
SHOW DATABASES;
USE ssb;
SHOW TABLES;
SELECT COUNT(*) FROM date;
```

## 使用须知

- Schema 和表已固定为内置的 benchmark 定义，因此您无法在此 catalog 中创建、更改或删除对象。
- 数据是在查询时动态生成的，不存储在任何外部系统中。