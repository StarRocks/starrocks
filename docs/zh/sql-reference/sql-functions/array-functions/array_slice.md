---
displayed_sidebar: "Chinese"
---

# array_slice

## 功能

返回数组的一个数组片段。根据 `offset` 指定的位置，从 `input` 中截取长度为 `length` 的数组片段。

## 语法

```Haskell
array_slice(input, offset, length)
```

## 参数说明

* `input`：输入数组，类型为 ARRAY。数组元素可以是以下数据类型：BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2、VARCHAR、DATETIME、DATE、JSON。**从 2.5 版本开始，该函数支持 JSON 类型的数组元素。**
* `offset`: 结果数组片段的起始偏移 (**从 1 开始**)。支持的数据类型为 BIGINT。
* `length`: 结果数组片段的长度，即元素个数。支持的数据类型为 BIGINT。

## 返回值说明

返回值的数据类型为 ARRAY (元素类型与输入 `input` 保持一致)。

## 注意事项

* 偏移量从 1 开始。
* 如果指定的截取长度大于实际能截取的长度，则返回全部符合条件的元素，见示例四。

## 示例

**示例一**

```plain text
mysql> select array_slice([1,2,4,5,6], 3, 2) as res;
+-------+
| res   |
+-------+
| [4,5] |
+-------+
```

**示例二**

```plain text
mysql> select array_slice(["sql","storage","query","execute"], 1, 2) as res;
+-------------------+
| res               |
+-------------------+
| ["sql","storage"] |
+-------------------+
```

**示例三**

```plain text
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 3) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```

**示例四**

```plain text
mysql> select array_slice([57.73,97.32,128.55,null,324.2], 3, 5) as res;
+---------------------+
| res                 |
+---------------------+
| [128.55,null,324.2] |
+---------------------+
```

以上示例中，指定的长度为 5， 但是只有 3 个元素符合条件，返回全部 3 个元素。
