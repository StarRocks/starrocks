---
displayed_sidebar: "Chinese"
---

# array_filter

## 功能

根据设定的过滤条件返回数组中匹配的元素。该函数可用于普通的数组过滤，也可以搭配 Lambda 函数进行更灵活的数组过滤。有关 Lambda 表达式的详细信息，参见 [Lambda expression](../Lambda_expression.md)。该函数从 2.5 版本开始支持。

## 语法

```Haskell
array_filter(array, array<bool>)
array_filter(lambda_function, arr1,arr2...)
```

- `array_filter(array, array<bool>)`

   将 `array` 数组中的每个元素传递给 `array<bool>` 函数进行判断。 如果 `array<bool>` 返回 `true`，则将 `array` 中的当前元素返回到结果数组中。

- `array_filter(lambda_function, arr1,arr2...)`

   将 `lambda_function` 应用到每个输入数组，返回匹配的数组。

## 参数说明

- `array`: 要进行过滤的数组。

- `array<bool>`: 用于过滤数组的表达式。

- `lambda_function`: 用于过滤数组的 lambda 表达式。

## 注意事项

- `array_filter(array, array<bool>)` 里的两个输入参数必须是 ARRAY 类型，并且第二个参数可以兼容或者转换成 `array<bool>`。
- `array_filter(lambda_function, arr1,arr2...)` 中对 lambda function 的要求同 [array_map()](./array_map.md)。
- 如果输入数组为 null，返回 null。如果用于过滤的数组为 null，则返回空数组。

## 示例

### 不使用 lambda 表达式

  ```plain
    -- 数组中的所有元素都匹配过滤规则，返回数组中的所有元素。
    select array_filter([1,2,3],[1,1,1]);
    +------------------------------------+
    | array_filter([1, 2, 3], [1, 1, 1]) |
    +------------------------------------+
    | [1,2,3]                            |
    +------------------------------------+
    1 row in set (0.01 sec)
    
    -- 返回和过滤规则匹配的元素，第一和第三个元素匹配。
    select array_filter([1,2,3],[1,0,1]);
    +------------------------------------+
    | array_filter([1, 2, 3], [1, 0, 1]) |
    +------------------------------------+
    | [1,3]                              |
    +------------------------------------+
    1 row in set (0.01 sec)
    
    -- 过滤数组为 null，返回空数组。
    select array_filter([1,2,3],null);
    +-------------------------------+
    | array_filter([1, 2, 3], NULL) |
    +-------------------------------+
    | []                             |
    +-------------------------------+
    1 row in set (0.01 sec)
    
    -- 输入数组为 null，返回 null。
    select array_filter(null,[1]);
    +-------------------------+
    | array_filter(NULL, [1]) |
    +-------------------------+
    | NULL                    |
    +-------------------------+
    
    -- 输入数组和过滤数组都是 null，返回 null。
    select array_filter(null,null);
    +--------------------------+
    | array_filter(NULL, NULL) |
    +--------------------------+
    | NULL                     |
    +--------------------------+
    1 row in set (0.01 sec)
    
    -- 过滤数组中的元素为 null，返回空数组。
    select array_filter([1,2,3],[null]);
    +---------------------------------+
    | array_filter([1, 2, 3], [NULL]) |
    +---------------------------------+
    | []                              |
    +---------------------------------+
    1 row in set (0.01 sec)
    
    -- 过滤数组中的两个元素都为 null，返回空数组。
    select array_filter([1,2,3],[null,null]);
    +---------------------------------------+
    | array_filter([1, 2, 3], [NULL, NULL]) |
    +---------------------------------------+
    | []                                    |
    +---------------------------------------+
    1 row in set (0.00 sec)
    
    -- 仅返回数组中符合条件的元素 2。
    select array_filter([1,2,3],[null,1,0]);
    +---------------------------------------+
    | array_filter([1, 2, 3], [NULL, 1, 0]) |
    +---------------------------------------+
    | [2]                                   |
    +---------------------------------------+
  ```

### 使用 Lambda 表达式

  ```plain
    -- 返回数组 `x` 中小于数组 `y` 的元素。
    select array_filter((x,y) -> x < y, [1,2,null], [4,5,6]);
    +--------------------------------------------------------+
    | array_filter((x, y) -> x < y, [1, 2, NULL], [4, 5, 6]) |
    +--------------------------------------------------------+
    | [1,2]                                                  |
    +--------------------------------------------------------+
  ```
