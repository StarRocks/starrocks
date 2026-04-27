---
displayed_sidebar: docs
---

# materialize

## 功能

返回输入值本身，但作为 FE（前端）优化器的优化屏障。将表达式包裹在 `materialize()` 中可以阻止优化器对该表达式执行常量折叠、分区裁剪等优化。

该函数主要用于测试和调试查询执行行为。例如，您可以使用它来验证在不依赖分区裁剪的情况下查询是否返回正确的结果。

## 语法

```Haskell
materialize(x);
```

## 参数说明

`x`：需要透传的表达式。支持的数据类型包括 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、VARCHAR、DATE、DATETIME、DECIMALV2、DECIMAL32、DECIMAL64、DECIMAL128、DECIMAL256、JSON 和 VARBINARY。

## 返回值说明

返回与输入相同的值和类型。

## 示例

阻止分区裁剪：

```Plain Text
-- 不使用 materialize：分区裁剪生效
SELECT * FROM sales WHERE dt >= '2024-01-01';

-- 使用 materialize：阻止对 dt 的分区裁剪
SELECT * FROM sales WHERE materialize(dt) >= '2024-01-01';
```

阻止常量折叠：

```Plain Text
SELECT materialize(1 + 2);
+-------------------+
| materialize(1 + 2)|
+-------------------+
|                 3 |
+-------------------+
```

## 关键字

MATERIALIZE, materialize
