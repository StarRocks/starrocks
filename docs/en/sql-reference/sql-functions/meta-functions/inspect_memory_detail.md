---
displayed_sidebar: docs
---

# inspect_memory_detail

`inspect_memory_detail(module_name, class_info)`

This function returns the estimated memory usage for a specific class or field within a module.

## Arguments

`module_name`: The name of the module (VARCHAR).
`class_info`: The name of the class or 'class_name.field_name' (VARCHAR).

## Return Value

Returns a VARCHAR string representing the estimated memory size (e.g., "100MB").

