---
displayed_sidebar: docs
---

# map_from_arrays

## 功能

将两个 ARRAY 数组作为 Key 和 Value 组合成一个 MAP 对象。

该命令从 3.0 版本开始支持。

## 语法

```Haskell
MAP map_from_arrays(ARRAY keys, ARRAY values)
```

## 参数说明

- `keys`: 用于生成 MAP 中的 Key 值。`keys` 中的元素必须唯一。
- `values`: 用于生成 MAP 中的 Value 值.

## 返回值说明

返回一个 MAP 值，Map 中的 Key 为 `keys` 中的元素，Map 中的 Value 为 `values` 中的元素。

返回规则如下：

- `keys` 和 `values` 长度（元素个数）必须相同，否则返回报错。

- 如果 `keys` 或者 `values` 为 NULL, 则返回 NULL。

## 示例

```Plaintext
select map_from_arrays([1, 2], ['Star', 'Rocks']);
+--------------------------------------------+
| map_from_arrays([1, 2], ['Star', 'Rocks']) |
+--------------------------------------------+
| {1:"Star",2:"Rocks"}                       |
+--------------------------------------------+
```

```Plaintext
select map_from_arrays([1, 2], NULL);
+-------------------------------+
| map_from_arrays([1, 2], NULL) |
+-------------------------------+
| NULL                          |
+-------------------------------+
```
