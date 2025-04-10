---
displayed_sidebar: docs
---

# cosine_similarity

## 功能

计算两个向量的余弦夹角来评估向量之间的相似度。

相似度取值在 -1 到 1 之间。如果两个向量的夹角为 0°，即两个向量方向重合时，相似度为 1；如果夹角为 180°，即两个向量方向相反时，相似度为 -1；如果夹角为 90°，即两个向量方向垂直时，相似度为 0。

值越趋近于 1，代表两个向量的方向越接近；越趋近于 -1，方向越相反；越趋近于 0，表示两个向量近乎于正交。

余弦相似度在度量文本相似度、视频相似度等场景都较为常用。

该函数会对向量做归一化处理，然后计算余弦相似度。如果输入的向量已经做了归一化，可以使用 [cosine_similarity_norm](./cos_similarity_norm.md)。

## 语法

```Haskell
cosine_similarity(a, b)
```

## 参数说明

`a` 和 `b` 是进行比较的两个向量，维度必须相同。取值必须是 `Array<float>` 类型，即 Array 中的元素仅支持 FLOAT 类型。注意两个数组的元素个数必须相同，否则返回报错。

## 返回值说明

返回 FLOAT 类型的值，取值范围 [-1, 1]。如果输入参数为 NULL 或者类型无效时，返回报错。

## 示例

1. 创建表来存储向量，并向表中插入数据。

   ```SQL
      CREATE TABLE t1_similarity 
      (id int, data array<float>)
      DISTRIBUTED BY HASH(id);
      
      INSERT INTO t1_similarity VALUES
      (1, array<float>[0.1, 0.2, 0.3]), 
      (2, array<float>[0.2, 0.1, 0.3]), 
      (3, array<float>[0.3, 0.2, 0.1]);
    ```

2. 使用 cosine_similarity 函数来计算 `data` 列每行的值相对于数组 `[0.1, 0.2, 0.3]` 的相似度，并以降序排列结果。

    ```Plain
      SELECT id, data, cosine_similarity(array<float>[0.1, 0.2, 0.3], data) as dist
      FROM t1_similarity 
      ORDER BY dist DESC;
      
      +------+---------------+-----------+
      | id   | data          | dist      |
      +------+---------------+-----------+
      |    1 | [0.1,0.2,0.3] | 0.9999999 |
      |    2 | [0.2,0.1,0.3] | 0.9285713 |
      |    3 | [0.3,0.2,0.1] | 0.7142856 |
      +------+---------------+-----------+
    ```
