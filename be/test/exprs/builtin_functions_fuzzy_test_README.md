# Built-in Functions Fuzzy Test

## 概述

`builtin_functions_fuzzy_test.cpp` 是一个全面的模糊测试（fuzzy test），用于测试 StarRocks BE 中所有 built-in function 的健壮性。该测试的主要目标是确保函数在接收各种不匹配的输入类型时不会发生 crash，从而提高系统的稳定性。

## 设计目标

1. **自动发现函数**：从 `BuiltinFunctions::get_all_functions()` 自动获取所有已注册的 built-in function
2. **全面的类型覆盖**：测试所有 Column 类型，包括基础类型和复合类型
3. **类型组合测试**：测试 Const、Nullable 等包装器的各种组合
4. **参数数量变化**：测试函数在接收错误参数数量时的行为
5. **可扩展性**：当新增 built-in function 时，测试会自动覆盖，无需手动修改

## 测试覆盖的 Column 类型

### 基础类型
- `TYPE_BOOLEAN`
- `TYPE_TINYINT`, `TYPE_SMALLINT`, `TYPE_INT`, `TYPE_BIGINT`, `TYPE_LARGEINT`
- `TYPE_FLOAT`, `TYPE_DOUBLE`
- `TYPE_VARCHAR`, `TYPE_VARBINARY`
- `TYPE_DATE`, `TYPE_DATETIME`
- `TYPE_DECIMAL32`, `TYPE_DECIMAL64`, `TYPE_DECIMAL128`
- `TYPE_JSON`

### 复合类型
- **Array 类型**：`ARRAY_*` 各种元素类型的数组
- **Map 类型**：`MAP_*` 键值对映射（未来扩展）
- **Struct 类型**：结构体类型（未来扩展）

### 包装器类型
- **Nullable**：可空包装器，可以与任何基础类型组合
- **Const**：常量包装器，可以与任何基础类型组合
- **Nullable + Const**：两种包装器的组合

## 测试策略

### 1. 随机数据生成
- 为每种类型生成合理范围内的随机数据
- 字符串类型生成随机长度的字符串
- 数值类型在合理范围内生成随机值
- 日期时间类型生成有效的时间戳

### 2. 类型不匹配测试
- 随机选择不同的输入类型组合
- 测试函数签名不匹配的情况
- 测试参数数量不匹配的情况

### 3. 边界条件测试
- 空 Column 测试
- 全 NULL Column 测试
- 混合 NULL 和非 NULL 数据测试

### 4. 异常处理验证
- 函数应该优雅地处理类型不匹配，返回错误而不是 crash
- 捕获并记录异常，但不将其视为测试失败
- 只有未预期的异常类型（如 segfault）才被视为真正的问题

## 测试用例组织

### TestAllBuiltinFunctions
遍历所有 built-in function，对每个函数进行基础的随机输入测试。

### TestSpecificFunctionTypes
按功能分类测试函数：
- Array 函数
- Map 函数  
- JSON 函数
- 时间函数
- 字符串函数
- 其他函数

对每个分类中的部分函数进行更深入的测试。

### TestNullAndEmptyInputs
专门测试边界条件：
- 空 Column 输入
- 全 NULL Column 输入

## 预期行为

### 正常情况
- 函数接收正确类型的输入时应该正常执行
- 返回的 Column 应该非空且大小正确

### 类型不匹配情况
- 函数应该返回错误状态而不是 crash
- 可能抛出 `std::exception` 类型的异常，这是预期的
- 不应该抛出未处理的异常或发生 segfault

### 参数数量不匹配情况
- 函数应该优雅地处理参数数量错误
- 应该返回错误而不是访问无效内存

## 使用方法

### 编译
```bash
cd /workspace/be/build
make builtin_functions_fuzzy_test
```

### 运行
```bash
# 运行所有 fuzzy 测试
./test/builtin_functions_fuzzy_test

# 运行特定测试用例
./test/builtin_functions_fuzzy_test --gtest_filter="*TestAllBuiltinFunctions*"
```

### CI 集成
该测试被设计为可以在 CI 中运行的独立可执行文件，可以作为回归测试的一部分。

## 扩展性

### 添加新的 Column 类型
在 `get_all_logical_types()` 函数中添加新类型，并在 `create_random_column()` 中添加对应的创建逻辑。

### 添加新的测试策略
可以添加新的测试函数，例如：
- 性能压力测试
- 内存泄漏检测
- 并发安全测试

### 自定义测试数据
可以扩展随机数据生成器，添加更多有针对性的测试数据模式。

## 注意事项

1. **随机种子固定**：测试使用固定的随机种子（42），确保测试结果可重现
2. **异常处理**：测试区分预期异常和真正的问题，只有后者会导致测试失败
3. **内存管理**：所有 Column 使用智能指针管理，避免内存泄漏
4. **测试时间**：由于要测试大量函数和类型组合，测试可能需要较长时间

## 故障排查

### 如果测试失败
1. 检查是否有函数抛出了未预期的异常类型
2. 查看日志输出，确定是哪个函数和哪种输入组合导致的问题
3. 检查相关函数的输入类型检查逻辑

### 如果测试运行时间过长
1. 可以减少测试组合的数量
2. 可以选择性地跳过某些函数类别
3. 可以调整随机数据的大小

## 贡献指南

当添加新的 built-in function 时：
1. 确保函数在 `BuiltinFunctions::_fn_tables` 中正确注册
2. 确保函数有适当的输入类型检查
3. 运行 fuzzy test 验证函数的健壮性
4. 如果发现 crash，修复函数的输入验证逻辑