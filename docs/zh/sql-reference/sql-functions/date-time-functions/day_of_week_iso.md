---
displayed_sidebar: docs
---

# dayofweek_iso

## 功能

根据 ISO 标准，计算某一个日期对应的是一周内的星期几，并返回一个整数值，范围在 `1` 到 `7` 之间。其中，`1` 代表星期一，`7` 代表星期日。

## 语法

```Haskell
INT DAY_OF_WEEK_ISO(DATETIME date)
```

## 参数说明

`date`：待计算转换的日期，取值必须是 DATE 或 DATETIME 数据类型。

## 示例

根据 ISO 标准，计算并返回日期 `2023-01-01` 对应的是一周内的星期几：

```SQL
MySQL > select dayofweek_iso('2023-01-01');
+-----------------------------+
| dayofweek_iso('2023-01-01') |
+-----------------------------+
|                           7 |
+-----------------------------+
```

## 关键字

DAY_OF_WEEK_ISO
