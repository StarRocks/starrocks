<<<<<<< HEAD
# 如何贡献文档

非常感谢您参与 StarRocks 文档建设，您的帮助是文档改进的关键！
贡献文档前，请您仔细阅读本文，帮助您快速了解文档的注意事项、编写流程和文档模版。

## 注意事项
=======
# StarRocks 文档

欢迎来到 StarRocks 文档仓库！本仓库存放的是 StarRocks 官网中文文档的源文件。官网英文文档的源文件存放于 [StarRocks/starrocks/docs](https://github.com/StarRocks/starrocks/tree/main/docs)。

如果您在使用 StarRocks 文档时发现文档问题，可以直接在 [文档官网](https://docs.starrocks.io/zh-cn/latest/introduction/StarRocks_intro) 右上角点击 “反馈” 按钮提交 Issue，或者直接提交 Pull Request 来进行修改。StarRocks 团队会及时修复您提交的问题和 PR。详细的问题反馈和 PR 提交流程，参见 [如何贡献文档和反馈意见](feedback-and-contribute.md)。我们期待您的宝贵意见！
>>>>>>> e1acf16 (fix external/other fag, cbo and readme (#3657))

如果您对产品使用或功能有疑问，或者想获取更多技术支持，可以在 [StarRocks 代码库](https://github.com/StarRocks/starrocks/issues) 提交 issue 或者加入 StarRocks 社区群。

<<<<<<< HEAD
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

[函数文档模板](./sql-reference/sql-functions/How%20to%20Write%20Functions%20Documentation.md)
[SQL命令模板](./sql-reference/How%20to%20write%20SQL%20command%20doc.md)
=======
## StarRocks 文档分支和版本说明

StarRocks 文档维护在以下分支，对应官网文档的不同版本。

| 文档仓库 branch                                            | 对应文档版本         |
| -------------------------------------------------------   | ----------------   |
| [main](https://github.com/StarRocks/docs.zh-cn/tree/main) | Main 版            |
| [2.5](https://github.com/StarRocks/docs.zh-cn/tree/2.5)   | RC 版              |
| [2.4](https://github.com/StarRocks/docs.zh-cn/tree/2.4)   | Latest-2.4         |
| [2.3](https://github.com/StarRocks/docs.zh-cn/tree/2.3)   | 2.3                |
| [2.2](https://github.com/StarRocks/docs.zh-cn/tree/2.2)   | 2.2 长期支持版 (LTS) |
| [2.1](https://github.com/StarRocks/docs.zh-cn/tree/2.1)   | 2.1                |
| [2.0](https://github.com/StarRocks/docs.zh-cn/tree/2.0)   | 2.0                |
| [1.19](https://github.com/StarRocks/docs.zh-cn/tree/1.19) | 1.19               |
| [1.18](https://github.com/StarRocks/docs.zh-cn/tree/1.18) | 无                 |
>>>>>>> e1acf16 (fix external/other fag, cbo and readme (#3657))
