# element_at

## 功能

获取 Map 中指定键 (Key) 对应的值 (Value)。如果输入值为 NULL 或指定的 Key 不存在，则返回 NULL。

如果您想从 Array 数据中获取指定位置的元素，参见 Array 函数中的 [element_at](../array-functions/element_at.md)。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
element_at(any_map, any_key)
```

## 参数说明

- `any_mao`: Map 表达式。
- `key`: 指定的 key。如果 `key` 不存在，返回 NULL。

## 返回值说明

返回 `key` 对应的 Value 值。

## 示例

```plain text
mysql> select element_at(map{1:3,2:4},1);
+-------------------------+
| element_at({1:3,2:4},1) |
+-------------------------+
|                     3   |
+-------------------------+

mysql> select element_at(map{1:3,2:4},3);
+-------------------------+
| element_at({1:3,2:4},3) |
+-------------------------+
|                    NULL |
+-------------------------+

mysql> select element_at(map{'a':1,'b':2},'a');
+-----------------------+
| map{'a':1,'b':2}['a'] |
+-----------------------+
|                     1 |
+-----------------------+
```

## keywords

ELEMENT_AT, MAP
