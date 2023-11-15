# array_concat

## 功能

拼接多个数组。

## 语法

```Haskell
output array_concat(input0, input1, ...)
```

## 参数说明

* input：不限数量(1个或多个)的具有相同元素类型的数组(input0, input1, ...)，具体元素类型可以任意。

## 返回值说明

类型为Array(元素类型与input中array的元素类型保持一致)，内容为(input0, input1, ...)中所有元素有序拼接后构成的数组。

## 示例

**示例一**:

```plain text
mysql> select array_concat([57.73,97.32,128.55,null,324.2], [3], [5]) as res;
+-------------------------------------+
| res                                 |
+-------------------------------------+
| [57.73,97.32,128.55,null,324.2,3,5] |
+-------------------------------------+
```

**示例二**:

```plain text
mysql> select array_concat(["sql","storage","execute"], ["Query"], ["Vectorized", "cbo"]);
+----------------------------------------------------------------------------+
| array_concat(['sql','storage','execute'], ['Query'], ['Vectorized','cbo']) |
+----------------------------------------------------------------------------+
| ["sql","storage","execute","Query","Vectorized","cbo"]                     |
+----------------------------------------------------------------------------+
```

**示例 三**:

```plain text
mysql> select array_concat(["sql",null], [null], ["Vectorized", null]);
+---------------------------------------------------------+
| array_concat(['sql',NULL], [NULL], ['Vectorized',NULL]) |
+---------------------------------------------------------+
| ["sql",null,null,"Vectorized",null]                     |
+---------------------------------------------------------+
```

## 关键字

ARRAY_CONCAT
