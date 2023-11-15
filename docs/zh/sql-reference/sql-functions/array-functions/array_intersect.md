# array_intersect

## 功能

对于多个同类型数组，返回交集。

## 语法

```Haskell
output array_intersect(input0, input1, ...)
```

## 参数说明

* input：不限数量(1个或多个)的具有相同元素类型的数组(input0, input1, ...)，具体类型可以任意。

## 返回值说明

类型为Array(元素类型与input中数组的元素类型保持一致)，内容为所有输入数组(input0, input1, ...)的交集。

## 示例

**示例一**:

```plain text
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", "query", "SQL"], ["SQL"])
AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| ["SQL"]      |
+--------------+
```

**示例二**:

```plain text
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| []           |
+--------------+
```

**示例 三**:

```plain text
mysql> SELECT array_intersect(["SQL", null, "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| [null]       |
+--------------+
```
