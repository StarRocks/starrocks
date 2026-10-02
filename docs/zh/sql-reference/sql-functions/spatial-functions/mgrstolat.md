---
displayed_sidebar: docs
description: "返回 MGRS 字符串所标识格网方格中心点的纬度。"
---

# MGRSToLat

解码[军事格网参照系（MGRS）](https://en.wikipedia.org/wiki/Military_Grid_Reference_System)字符串，返回所引用格网方格中心点的纬度。这是 `geoToMGRS` 的逆函数之一；另见 `MGRSToLng`。

输入不区分大小写，空格将被忽略。

## 语法

```Haskell
DOUBLE MGRSToLat(VARCHAR mgrs)
```

## 参数说明

- `mgrs`：MGRS 参考字符串，例如 `'31UDQ4825111935'`。支持的数据类型为 VARCHAR。

## 返回值说明

返回 DOUBLE 类型，表示格网方格中心点的纬度（度，WGS84）。如果参数为 NULL 或字符串格式不合法，则返回 NULL。

## 示例

```sql
SELECT MGRSToLat('31UDQ4825111935');
+-----------------------------+
| MGRSToLat('31UDQ4825111935')|
+-----------------------------+
| 48.85822536113692           |
+-----------------------------+
```

## 关键词

MGRSTOLAT,MGRS,GEO,SPATIAL,UTM,LATITUDE
