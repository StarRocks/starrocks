# How to feedback and contribute

非常感谢您参与 StarRocks 文档建设，您的帮助和意见是文档改进的关键！<br />
贡献文档或反馈意见前，请您仔细阅读本文，快速了解注意事项、问题提交和编写流程，以及文档模版。

## 反馈意见

如果您在阅读文档时，发现文档里描述有误或不规范，可以直接在官网文档右上角单击**反馈**，提交 GitHub issue。在issue里提供必要的截图，问题描述和建议修改，然后提交问题。<br />
![image](https://user-images.githubusercontent.com/98087056/183545340-205e3730-f2a7-4e45-bfef-63afb356b814.png)

## 提交PR修改文档

如果您想直接修改或更新文档里的描述，可提交PR贡献修改。

1. 进入文档编辑页面。
   - 方式一：在官网文档中，单击右上角的**编辑**，选择对应的文档分支后（一般为 main 分支），进入文档编辑页面。
   - 方式二：登录 GitHub 帐户，直接在[中文文档仓库](https://github.com/StarRocks/docs.zh-cn)找到对应文档，单击文档右上角的铅笔图标，进入文档编辑页面。<br />
   ![image](https://user-images.githubusercontent.com/98087056/183545639-cdea3e25-5fee-445e-8de9-4ec4aa583828.png)

2. 完成文档编辑后，在页面底部的 Propose changes 区域中，输入此次文档变更的标题或更多描述。
   ![image](https://user-images.githubusercontent.com/98087056/183545158-c5dd5e53-37c8-482b-8d15-51c2a21689cb.png)

3. 单击 Propose changes，提交文档变更。
4. 新建PR，输入PR标题和描述，然后单击 Create pull request。
   ![image](https://user-images.githubusercontent.com/98087056/183552303-0853e1aa-3948-49e1-8240-7d30b6d7809b.png)

5. PR 进入评审阶段，评审通过后，文档变更会合入至 StarRocks 文档仓库，并最终更新至官网。
   评审阶段分为自动检查和人工评审。
   1. 自动检查：提交人是否已经签署[CLA](https://cla-assistant.io/StarRocks/starrocks)，提交文档是否符合markdown语法。
   2. 人工评审：StarRocks Commiter会阅读并沟通文档内容。最终合入至StarRocks文档仓库，并更新官网。

## 贡献新文档

### 注意事项

1. StarRocks文档仓库使用markdown格式，贡献文档前，请熟悉基本的markdown语法。
2. 语言：请至少选择中英文一种语言，欢迎提供中英双语版本。
3. 索引：如果文档为新增，则需要在索引文件（TOC.md）中根据分类为新增文档添加索引。此索引文件最终将被渲染为官网页面的侧边导航。
4. 图片：图片需要先放入文件 assets 中，文档中引用图片时请输入图片的相对路径，比如 `![test image](../../assets/test.png)` 。
5. 引用：如果引用官网文档，推荐使用文档相对路径，比如 `[test md](../../sql-referencest.md)`。<br /> 如果引用外部文档，则引用格式为 `[引用文档名称](引用文档链接)`。
6. 代码块：代码块必须标明语言类型，比如：```sql。
7. 文档中目前暂不支持出现特殊符号。

### 编写流程

1. 登录 GitHub 帐户，直接在[中文文档仓库](https://github.com/StarRocks/docs.zh-cn)单击 Add file，进入新文档编辑页面。
   ![image](https://user-images.githubusercontent.com/98087056/183546267-f05f6afc-4d58-40f8-ba73-437f82d5f662.png)

2. 编写文档（markdown格式），并在索引（TOC.md）中添加索引。
    > 由于文档为markdown格式，建议您提前使用markdown-lint验证格式的规范性。
3. 在github上提交文档Propose new file，输入标题和描述，然后提交，创建PR。
   ![image](https://user-images.githubusercontent.com/98087056/183547476-bf3adca9-dde9-4205-a2a9-ea6210e9ba48.png)

4. PR 进入评审阶段，评审通过后，文档变更会合入至 StarRocks 文档仓库，并最终更新至官网。

## 文档模板

[函数文档模板](./sql-reference/sql-functions/How_to_Write_Functions_Documentation.md)<br />
[SQL命令模板](./sql-reference/How_to_write_SQL_command_doc.md)
