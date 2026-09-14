---
displayed_sidebar: docs
description: "版本命名在版本控制文档中详细说明。"
sidebar_position: 1
---

# 版本发布指南

版本命名的详细信息请参阅[版本控制](../introduction/versioning.md)文档。首先阅读该页面以了解**主版本**、**次版本**和**补丁**版本的命名规则。

## 发布计划
- 大约每四个月发布一个次版本。
- 维护最新的三个次版本（**次版本**是点分版本中的第二个数字，例如，在3.4.2中，**4**是次版本）。

  由于每四个月发布一个次版本，预计一个次版本的支持时间最长为一年。

- 在维护中的次版本发布后2-3周内发布一个补丁版本。

## Pull Request 类型

StarRocks 中的每个 pull request 都应以类型命名，包括 **feature**、**enhancement** 和 **bugfix**。

### Feature

- 定义：Feature 是数据库中之前不存在的新功能或能力。它增加了新的行为或显著扩展了现有功能。
- 示例：
  - 添加一种新的数据结构类型（例如，新的表模型或索引类型）。
  - 实现新的查询语言功能（例如，新的 SQL 函数或 Operator）。
  - 引入新的 API 端点或接口以与数据库交互。

### Enhancement

- 定义：Enhancement 是对现有功能或功能的改进。它不会引入全新的行为，但会使现有功能更好、更快或更高效。
- 示例：
  - 优化查询执行计划的性能。
  - 改进数据库管理工具的用户界面。
  - 通过添加更细粒度的访问控制来增强安全功能。

### Bugfix

- 定义：Bugfix 是对现有代码中错误或缺陷的修正。它解决了阻止数据库正常或按预期运行的问题。
- 示例：
  - 修复在某些查询条件下发生的崩溃。
  - 更正查询返回的不正确结果。
  - 解决内存泄漏或资源管理问题。

## Cherry-pick 规则

我们为次版本定义了一些状态以协助 cherry-pick 管理。您可以在 `.github/.status` 文件中找到版本状态。

例如，在本文档发布时，StarRocks 版本 3.4 处于 `feature-freeze` 状态，而版本 3.3 是 `bugfix-only`。要验证这一点：

```bash
git switch branch-3.3
cat .github/.status
```

```bash
bugfix-only
```

1. `open`: 所有类型的 pull request 都可以合并，包括 feature、enhancement 和 bugfix。
2. `feature-freeze`: 只有 enhancement 和 bugfix pull request 可以合并。
3. `bugfix-only`: 只有 bugfix pull request 可以合并。
4. `code-freeze`: 除关键 CVE 修复外，不可合并任何 pull request。

次版本状态会随着一些基线触发器而变化，如下所示，并且如果需要，也可以提前更改。

1. 当次版本分支创建时，它变为 `open`，并保持 `open` 直到发布。
2. 当次版本发布时，它变为 `feature-freeze`。
3. 当下一个次版本发布时，前一个次版本变为 `bugfix-only`。
4. 次版本保持 `bugfix-only` 状态，直到再发布三个次版本，然后变为 `code-freeze`。

### 示例

- branch-5.1 被创建，该分支处于 `open` 状态，直到通过候选发布并公开发布。
- 一旦版本 5.1 公开发布，它进入 `feature-freeze` 状态。
- 一旦版本 5.2 公开发布，5.1 切换到 `bugfix-only`。
- 当版本 5.1、5.2、5.3 和 5.4 全部发布时：
  - 5.4 处于 `feature-freeze` 状态
  - 5.3 处于 `bugfix-only` 状态
  - 5.2 也处于 `bugfix-only` 状态
  - 5.1 处于 `code-freeze` 状态

## JDK 支持策略

StarRocks 的 FE、BE 和 CN 节点依赖 JDK 运行。StarRocks 要求和推荐的 JDK 版本遵循 [Eclipse Adoptium](https://adoptium.net/support) 发布的 Java LTS 日历，而不是 StarRocks 的版本号。各 StarRocks 版本对应的 JDK 版本，请参阅[环境配置](../deployment/preparation/environment_configurations.md)。

每个 StarRocks 次版本定义两个 JDK 版本：

- **最低 JDK**：Adoptium 仍在构建的最旧 Java LTS 版本。FE 无法在更低版本的 JDK 上启动。BE 和 CN 在更低版本的 JDK 上会记录错误，且 Java UDF、基于 JNI 的 Connector 等依赖 Java 的功能不受支持。
- **推荐 JDK**：下一个 Java LTS 版本。官方容器镜像使用该 JDK，并且在下一次变更时它将成为最低 JDK。使用低于推荐 JDK 的版本启动时，会打印弃用警告并指明推荐的 JDK 版本。

比推荐 JDK 更新的 Java LTS 版本尚不在本策略覆盖范围内。它们通常可以正常运行，但未经测试。非 LTS 的 Java 版本不受支持。

当前最低 JDK 为 JDK 17，推荐 JDK 为 JDK 21。

### 最低 JDK 何时变更

- 当前最低 JDK 在 Adoptium 上到达可用期结束（End of Availability）时，最低 JDK 发生变更。此时，原推荐 JDK 成为最低 JDK，下一个 Java LTS 版本成为推荐 JDK。
- 推荐 JDK 会在成为最低 JDK 之前整整一个周期公布，因此您可以通过本策略、JDK 版本对照表和启动警告获得约两年的提前通知。
- 变更在该日期之后发布的第一个次版本中生效，且前提是上一个次版本已经针对新的最低 JDK 打印了启动警告。否则，变更推迟到下一个次版本。
- 一个次版本在其整个生命周期内保持相同的最低 JDK 和推荐 JDK。补丁版本永不提高最低 JDK，因此升级补丁版本永不需要升级 JDK。对更新 JDK 的支持可以通过补丁版本添加到现有次版本中。

### 时间表

| 生效时间 | 最低 JDK | 推荐 JDK | 原因 |
| --- | --- | --- | --- |
| 2027 年 10 月 | JDK 21 | JDK 25 | Java 17 可用期结束 |
| 2029 年 12 月 | JDK 25 | JDK 29 | Java 21 可用期结束 |
| 2031 年 9 月 | JDK 29 | JDK 33 | Java 25 可用期结束 |

以上日期为 Adoptium 公布的可用期结束日期（Adoptium 注明为 "at least"，即不早于该日期）。这些日期是固定的：即使 Adoptium 之后延长某个 Java 版本的可用期，时间表也不会改变。Java 29 和 Java 33 假定 Adoptium 继续每两年指定一个 LTS 版本。

### 在其他 JVM 中运行的组件

Spark Load 的 DPP 库、Hive Bitmap UDF 库和 Broker 运行在其他系统的 JVM 中。它们以 Java 8 为目标，遵循所在系统的 JDK 要求，不适用本策略。
