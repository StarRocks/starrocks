---
displayed_sidebar: docs
---

# How to feedback and contribute

非常感谢您参与 StarRocks 文档建设，您的帮助和意见是文档改进的关键！

贡献文档或反馈意见前，请您仔细阅读本文，快速了解注意事项、问题提交和编写流程，以及文档模版。

## 反馈意见

如果您在阅读文档时，发现文档里描述有误、描述不清晰、或者不规范，可以直接在文档页面底部的 **文档是否有帮助** 中进行反馈，或者直接在左下角点击**编辑此页**，提交 PR 修改文档。

## 提交PR修改文档

如果您想直接修改或更新文档里的描述，可提交 PR 贡献修改。

1. 进入文档编辑页面。
   - 方式一：在官网文档中，单击页面左下角的**编辑此页**，选择对应的文档分支后（一般为 main 分支），进入文档编辑页面。
   - 方式二：登录 GitHub，直接在[中文文档仓库](https://github.com/StarRocks/starrocks/tree/main/docs/zh)找到对应文档，单击文档右上角的铅笔图标，进入文档编辑页面。

   ![image](https://user-images.githubusercontent.com/98087056/183545639-cdea3e25-5fee-445e-8de9-4ec4aa583828.png)

2. 完成文档编辑后，在页面底部的 Propose changes 区域中，输入此次文档变更的标题或更多描述。
   ![image](https://user-images.githubusercontent.com/98087056/183545158-c5dd5e53-37c8-482b-8d15-51c2a21689cb.png)

3. 单击 Propose changes，提交文档变更。

4. 新建PR，输入PR标题和描述，然后单击 Create pull request。

   ![image](https://user-images.githubusercontent.com/98087056/183552303-0853e1aa-3948-49e1-8240-7d30b6d7809b.png)

5. PR 进入评审阶段，评审通过后，文档变更会合入至 StarRocks 文档仓库，并最终更新至官网。
   评审阶段分为自动检查和人工评审。
   1. 自动检查：提交人是否已经签署[CLA](https://cla-assistant.io/StarRocks/starrocks)，提交文档是否符合markdown语法。
   2. 人工评审：StarRocks Committer 会阅读并沟通文档内容。最终合入至StarRocks文档仓库，并更新官网。

## 贡献新文档

### 注意事项

1. StarRocks文档仓库使用 markdown 格式，贡献文档前，请熟悉基本的 markdown 语法。
2. 语言：请至少选择中英文一种语言，欢迎提供中英双语版本。
3. 导航栏：如果文档为新增文档，则需要在 `docs/docusaurus` 下的 `sidebars.json` 文件中为新增文档添加中文和英文索引。如果您不熟悉此操作，提交 PR 后，文档团队也会自行添加索引。
4. 图片：图片需要先放入文件 assets 中，文档中引用图片时请输入图片的相对路径，比如 `![test image](../../_assets/test.png)` 。
5. 引用：如果引用官网文档，推荐使用文档相对路径，比如 `[test md](./data_source/catalog/hive_catalog.md)(../zh/data_source/catalog/hive_catalog.md)`。如果引用外部文档，则引用格式为 `[引用文档名称](引用文档链接)`。
6. 代码块：代码块必须标明语言类型，比如：`sql`，`json`。

### 编写流程

1. 登录 GitHub 帐户，直接在[中文文档仓库](https://github.com/StarRocks/starrocks/tree/main/docs/zh)单击 Add file，进入新文档编辑页面。
   ![image](https://user-images.githubusercontent.com/98087056/183546267-f05f6afc-4d58-40f8-ba73-437f82d5f662.png)

2. 编写文档（markdown 格式），并在 `sidebars.json` 文件中添加索引。

3. 在 Github 上提交文档 Propose new file，输入标题和描述，然后提交，创建 PR。
   ![image](https://user-images.githubusercontent.com/98087056/183547476-bf3adca9-dde9-4205-a2a9-ea6210e9ba48.png)

   > 请关注 PR 的 check 检查结果，如果有 failed check，请及时修复问题。

4. PR 进入评审阶段，评审通过后，文档变更会合入至 StarRocks 文档仓库，并最终更新至官网。

## 文档模板

- [函数文档模板](./sql-reference//How_to_Write_Functions_Documentation.md)
- [SQL命令模板](./sql-reference/How_to_write_SQL_command_doc.md)
- [参数/变量模板](./sql-reference/template_for_config.md)
