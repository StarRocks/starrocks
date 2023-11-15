# array_slice

## 功能

返回数组的一个数组片段。

## 语法

```Haskell
output array_slice(input, offset, length)
```

## 参数说明

* input：输入数组，类型为Array。
* offset: 结果数组片段的起始偏移(从1开始)。
* length: 结果数组片段的长度。

## 返回值说明

类型为Array(与输入input保持一致)，内容为input中以offset为首元素，长度为length的数组片段。

## 示例

**示例一**:

```plain text
mysql> select array_slice([1,2,4,5,6], 3, 2) as res;
+-------+
| res   |
+-------+
| [4,5] |
+-------+
```

**示例二**:

```plain text
mysql> select array_slice(["sql","storage","query","execute"], 1, 2) as res;
+-------------------+
| res               |
+-------------------+
| ["sql","storage"] |
+-------------------+
```

**示例 三**:

```plain text
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 3) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```

**示例 四**:

```plain text
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 5) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```

## 关键字

ARRAY_SLICE
