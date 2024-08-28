---
displayed_sidebar: docs
---

# 部署总览

本章节介绍如何在生产环境中部署、升级和降级 StarRocks 集群。

## 部署流程

以下为部署流程摘要，详细信息请参考各步骤文档。

StarRocks 的部署通常遵循以下步骤：

1. 确认 StarRocks 部署的[软硬件要求](../deployment/deployment_prerequisites.md)。

   在部署 StarRocks 之前，检查您的服务器必须满足哪些前提条件，包括 CPU、内存、存储、网络、操作系统和依赖项。

2. [规划集群规模](../deployment/plan_cluster.md)。

   规划集群中 FE 节点和 BE 节点的数量，以及服务器的硬件规格。

3. [检查环境配置](../deployment/environment_configurations.md)。

   当服务器准备就绪后，您需要在部署 StarRocks 之前检查并修改部分环境配置。

4. [准备部署文件](../deployment/prepare_deployment_files.md)。

   - 如需在基于 x86 架构的 CentOS 7.9 平台上部署 StarRocks，您可以直接下载并解压官网提供的软件包。
   - 如需在 ARM 架构 CPU 或 Ubuntu 22.04 上部署 StarRocks，您需要通过 StarRocks 的 Docker 镜像准备部署文件。
   - 如需在 Kubernetes 上部署 StarRocks，您可以跳过该步骤。

5. 部署 StarRocks。

   - 如需部署存算分离架构的 StarRocks 集群，请参考 [部署使用 StarRocks 存算分离集群](../deployment/shared_data/s3.md)。
   - 如需部署存算一体架构的 StarRocks 集群，则有以下选择：

     - [手动部署 StarRocks](../deployment/deploy_manually.md)。
     - [使用 Operator 部署 StarRocks 集群](../deployment/sr_operator.md)。
     - [使用 Helm 部署 StarRocks 集群](../deployment/helm.md)。

6. 执行必要的[部署后设置](../deployment/post_deployment_setup.md)措施。

   将 StarRocks 集群投入生产之前，您需要对集群进行进一步设置，包括管理初始帐户和设置部分性能相关的系统变量。

## 升级与降级

如果您计划将现有的 StarRocks 升级到更新版本而非首次安装 StarRocks，请参考 [升级 StarRocks](../deployment/upgrade.md) 了解升级方式和升级前应考虑的问题。

有关降级 StarRocks 集群的说明，请参阅 [降级 StarRocks](../deployment/downgrade.md)。
