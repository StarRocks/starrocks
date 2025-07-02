---
displayed_sidebar: docs
---

# inspect_memory_detail

`inspect_memory_detail(module_name, class_info)`

此函数返回模块中特定类或字段的估计内存使用量。

## 参数

`module_name`: 模块的名称 (VARCHAR)。
`class_info`: 类名或“class_name.field_name” (VARCHAR)。

## 返回值

返回表示估计内存大小的 VARCHAR 字符串（例如，“100MB”）。

