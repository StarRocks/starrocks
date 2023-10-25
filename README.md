# 如何贡献文档

非常感谢您参与 StarRocks 文档建设，您的帮助是文档改进的关键！
贡献文档前，请您仔细阅读本文，帮助您快速了解文档的注意事项、编写流程和文档模版。

## 注意事项

如果您在阅读文档时，发现文档里描述有误或不规范，可以直接在[官网文档](https://docs.starrocks.io/zh-cn/latest/introduction/StarRocks_intro) 右上角单击**反馈**，提交GitHub issue。在issue里提供必要的截图，问题描述和建议修改，然后提交问题。<br />
![image](https://user-images.githubusercontent.com/98087056/183545340-205e3730-f2a7-4e45-bfef-63afb356b814.png)

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
