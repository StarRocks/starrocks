---
displayed_sidebar: "Chinese"
---

# 如何贡献文档

非常感谢您参与 StarRocks 文档建设，您的帮助是文档改进的关键！
贡献文档前，请您仔细阅读本文，帮助您快速了解文档的注意事项、编写流程和文档模版。

## 注意事项

1. 语言：请至少选择中英文一种语言，欢迎提供中英双语版本。
2. 索引：如果文档为新增，则需要在索引文件（TOC.md）中根据分类为新增文档添加索引，且**索引路径为绝对路径**。此索引文件最终将被渲染为页面的侧边目录栏。
3. 图片：图片需要先放入文件 assets 中，文档中引用图片时请输入图片的相对路径，比如 `![test image](../../assets/test.png)` 。
4. 引用：如果引用官网文档，则推荐使用文档相对路径，比如 `[test md](../../sql-referencest.md)`。<br /> 如果引用外部文档，则引用格式为 `[引用文档名称](引用文档链接)`。
5. 代码块：代码块必须标明语言类型，比如：```sql。
6. 文档中目前暂不支持出现特殊符号。

## 编写流程

1. **编写阶段**：按照如下[文档模板](./README.md/##文档模板)，编写文档（markdown格式），并在索引（TOC.md）中添加索引。

    > - 由于文档为markdown格式，建议您提前使用markdown-lint验证格式的规范性。
    > - 添加索引时，请注意函数分类。

2. **提交阶段**：在github上提交文档，中文文档提交至[中文文档仓库](https://github.com/StarRocks/docs.zh-cn)，英文文档提交至[英文文档仓库](https://github.com/StarRocks/docs)。

3. **评审阶段**：

   评审阶段分为自动检查和人工评审。

   1. 自动检查：提交人是否已经签署CLA，提交文档是否符合markdown语法。
   2. 人工评审：Commiter会阅读并沟通文档内容。最终合入至StarRocks文档仓库，并更新官网。

## 文档模板

[函数文档模板](./sql-reference/sql-functions/How_to_Write_Functions_Documentation.md)
