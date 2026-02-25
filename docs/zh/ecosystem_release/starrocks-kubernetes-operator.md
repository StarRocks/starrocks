---
displayed_sidebar: docs
---

# Kubernetes Operator 版本发布

## 发布说明

StarRocks 提供的 Operator 用于在 Kubernetes 环境中部署 StarRocks 集群，集群组件包括 FE、BE 和 CN。

**使用文档**：

在 Kubernetes 上的部署 StarRocks 集群支持以下两种方式：

- [直接使用 StarRocks CRD 部署 StarRocks 集群](https://docs.starrocks.io/zh/docs/deployment/sr_operator/)
- [通过 Helm Chart 部署 Operator 和 StarRocks 集群](https://docs.starrocks.io/zh/docs/deployment/helm/)

**源码下载地址：**

[starrocks-kubernetes-operator and kube-starrocks Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator)

**资源下载地址:**

- **下载地址前缀**

   `https://github.com/StarRocks/starrocks-kubernetes-operator/releases/download/v${operator_version}/${resource_name}`

- **资源名称**
  - 定制资源 StarRocksCluster：**starrocks.com_starrocksclusters.yaml**
  - StarRocks Operator 默认配置文件：**operator.yaml**
  - Helm Chart，包括 `kube-starrocks` Chart `kube-starrocks-${chart_version}.tgz`。`kube-starrocks` Chart 还分两个子 Chart，`starrocks` Chart `starrocks-${chart_version}.tgz` 和 `operator` Chart `operator-${chart_version}.tgz`。

比如 1.8.6 版本 `kube-starrocks` Chart 的获取地址是：

`https://github.com/StarRocks/starrocks-kubernetes-operator/releases/download/v1.8.6/kube-starrocks-1.8.6.tgz`

**版本要求**

- Kubernetes：1.18 及以上
- Go：1.19 及以上

## 发布记录

### 1.9

#### 1.9.1

**功能改进**

- **[Helm Chart]** 当 `logStorageSize` 设置为 `0` 时，operator 不会为 log storage 创建 PersistentVolumeClaim（PVC）。[#398](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/398)
- **[Operator]** operator 会检查 `storageVolumes` 中 `mountPath` 和 `name` 的值是否重复。如果存在重复的值，则会返回报错提示。[#388](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/388)
- **[Operator]** FE 节点的数量不能缩减到 1。[#394](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/394)
- **[Operator]** 支持 merge 多个 values yaml 文件中定义`feEnvVars`、`beEnvVars` 和 `cnEnvVars` 的值。[#396](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/396)
- **[Operator]** 在 StarRocksCluster CRD 中添加了 `spec.containers.securityContext.capabilities`，以自定义容器的 Linux 权限。[#404](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/404)

**缺陷修复**

已修复以下问题：

- **[Operator]** 支持更新 `service` 中的 `annotations` 字段。[#402](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/402) [#399](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/399)
- **[Operator]** 使用 patch 而不是 update 的方式来修改 statefulset 和 deployment。这可以解决启用 CN + HPA 时升级 CN 会导致所有 CN pods 被终止并重新启动的问题。[#397](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/397)
- **[Operator]** 使用 patch 而不是 update 的方式来修改 service object。这样可以避免 operator 覆盖写入对  service object 的修改，例如，在使用 Kubernetes 云提供商时，该 Kubernetes 云提供商修改了 service object。[#387](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/387)

#### 1.9.0

**新增特性**

- 新增 StarRocksWarehouse CRD 以支持 StarRocks Warehouse。注意，StarRocks Warehouse 目前是 StarRocks 企业版的功能。[#323](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/323)

**功能改进**

- 在 StarRocksCluster CRD 中添加了 `status.reason` 字段。如果在部署集群过程中 subcontroller 的 apply 操作失败时，您可以执行 `kubectl get starrockscluster <name_of_the_starrocks_cluster_object> -oyaml`, 在返回结果中查看 `status.reason` 字段显示的错误日志。[#359](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/359)
- 在 storageVolumes 字段中可以挂载一个空目录。 [#324](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/324)

**缺陷修复**

已修复以下问题：

- StarRocks 集群的状态与集群的 FE、BE 和 CN 组件的状态不一致。[#380](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/380)
- 当删除 `autoScalingPolicy` 时，HPA 资源未删除。[#379](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/379)
- 当删除 `starRocksCnSpec` 时，HPA 资源未删除。[#357](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/357)

### 1.8

#### 1.8.8

**缺陷修复**

已修复以下问题：

- **[Operator]** 使用 `StarRocksFeSpec.service`、`StarRocksBeSpec.service` 和 `StarRocksCnSpec.service` 添加 annotations 时，Operator 不再向 search service （内部 service）添加相应的的 annotations。[#370](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/370)

### 1.8.7

**功能改进**

- 在 StarRocksCluster CRD 中添加了 `livenessProbeFailureSeconds` 和 `readinessProbeFailureSeconds` 字段。当 StarRocks 集群工作负载较重时，如果 liveness 和 readiness 探测的时间仍然为默认值，liveness 和 readiness 探测可能会失败，并导致容器重新启动。在这种情况下，您可以适当调大这两字段的值。[#309](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/309)

#### 1.8.6

**缺陷修复**

已修复以下问题：

在执行  Stream Load  作业时，返回错误 `sendfile() failed (32: Broken pipe) while sending request to upstream`。在 Nginx 将请求体发送给 FE 后，FE 会将请求重定向到 BE。此时，Nginx 中缓存的数据可能已经丢失。[#303](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/303)

**文档**

- [使用 FE proxy 从 Kubernetes 网络外部导入数据到 StarRocks 集群](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/load_data_using_stream_load_howto.md)
- [使用 Helm 更新 root 用户的密码](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/change_root_password_howto.md)

#### 1.8.5

**功能改进**

- **[Helm Chart] 支持为 Operator 的 service account 自定义注释和标签**。默认为 Operator 创建一个名为 `starrocks` 的 service account，用户可以通过在 **values.yaml** 文件中的 `serviceAccount` 中配置 `annotations` 和 `labels` 字段来自定义 service account `starrocks` 的注释和标签。`operator.global.rbac.serviceAccountName` 字段已被弃用。[#291](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/291)
- **[Operator] FE service 支持 Istio 的显式协议选择**。如果在 Kubernetes 环境中安装了 Istio，Istio 需要确定来自 StarRocks 集群的流量所使用的协议，以提供额外的功能，如路由和丰富的指标。因此， FE service 通过使用 `appProtocol` 字段显式声明其协议为 MySQL 协议。本改进尤为重要，因为 MySQL 协议是一种 server-first 协议，与自动协议检测不兼容，有时可能导致连接失败。[#288](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/288)

**缺陷修复**

已修复以下问题：

- **[Helm Chart]** 当 `starrocks.initPassword.enabled` 为 `true` 且指定了 `starrocks.starrocksCluster.name` 的值时，StarRocks 中的 root 用户密码无法初始化。这是因为 initpwd pod 使用错误的 FE service 域名连接 FE service。具体来说，在这种情况下，FE service 域名使用 `starrocks.starrocksCluster.name` 中指定的值，而 initpwd pod 仍然使用 `starrocks.nameOverride` 字段的值来组成 FE service 域名。[#292](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/292)

**升级说明**

- **[Helm Chart]** 当 `starrocks.starrocksCluster.name` 中指定的值与 `starrocks.nameOverride` 的值不同时，FE、BE 和 CN 的旧 `configmap` 会被删除，使用新名称的 `configmap` 会被创建。**这可能导致 FE/BE/CN pod 重新启动。**

#### 1.8.4

**新增特性**

- **[Helm Chart]** 可使用 Prometheus 和 ServiceMonitor CR 监控 StarRocks 集群的指标。使用文档，参见[与 Prometheus 和 Grafana 集成](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/integration/integration-prometheus-grafana.md)。[#284](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/284)
- **[Helm Chart]** 在 **values.yaml** 中的 `starrocksCnSpec` 中添加 `storagespec` 和相关字段，以配置 StarRocks 集群中 CN 节点的 log volume。[#280](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/280)
- 在 StarRocksCluster CRD 中添加 `terminationGracePeriodSeconds` 字段，以配置在删除或更新 StarRocksCluster 资源时优雅终止宽限期。[#283](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/283)
- 在 StarRocksCluster CRD 中添加 `startupProbeFailureSeconds` 字段，以配置 StarRocksCluster 资源中 pod 的启动探测失败阈值，启动探测在该时间内如果没有返回成功响应，则被视为失败。[#271](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/271)

**缺陷修复**

已修复以下问题：

- 当 StarRocks 集群中存在多个 FE pod 时，FE proxy 无法正确处理 STREAM LOAD 请求。[#269](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/269)

**文档**

- [快速开始：在本地部署 StarRocks 集群](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/local_installation_how_to.md)。
- [部署不同配置的 StarRocks 集群](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/examples/starrocks)。例如，[部署所有功能的 StarRocks 集群](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/examples/starrocks/deploy_a_starrocks_cluster_with_all_features.yaml)。
- [管理 StarRocks 集群的用户指南](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/doc)。例如，如何[配置日志和相关字段](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/logging_and_related_configurations_howto.md)以及[挂载外部 configmaps 或 secrets](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/mount_external_configmaps_or_secrets_howto.md)。

#### 1.8.3

**升级说明**

- **[Helm Chart]** 在默认的 **fe.conf** 文件中添加 `JAVA_OPTS_FOR_JDK_11` 。如果您使用了默认的 **fe.conf** 文件，则在升级到 v1.8.3 时，**会导致 FE Pod 的重启**。[#257](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/257)

**新增特性**

- **[Helm Chart]** 添加 `watchNamespace` 字段，指定 Operator 需要监视的唯一 namespace。否则，Operator 将监视 Kubernetes 集群中的所有 namespace。多数情况下，您不需要使用此功能。当 Kubernetes 集群管理较多节点，Operator 监视所有 namespace 并消耗太多内存资源时，您可以使用此功能。[#261](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/261)
- **[Helm Chart]** 在 **values.yaml** 中的 `starrocksFeProxySpec` 中添加 `Ports` 字段，允许用户指定 FE 代理服务的 NodePort。 [#258](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/258)

**功能改进**

- 在 **nginx.conf** 中 `proxy_read_timeout` 参数的值从 60s 更改为 600s，以避免超时。

#### 1.8.2

**功能改进**

- 提高 Operator pod 的内存使用上限，以避免内存溢出。[#254](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/254)

#### 1.8.1

**新增特性**

- 支持在 `configMaps` 和 `secrets` 中使用 `subpath` 字段，允许用户将文件挂载到指定的目录，且目录原有的内容不会被覆盖。[#249](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/249)
- 在 StarRocks 集群 CRD 中添加 `ports` 字段，允许用户自定义服务的端口。[#244](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/244)

**功能改进**

- 在删除 StarRocks 集群的 `BeSpec` 或 `CnSpec` 时，将相关的 Kubernetes 资源删除，确保集群的状态干净和一致。[#245](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/245)

#### 1.8.0

**升级说明和行为更改**

- **[Operator]** 要升级 StarRocksCluster CRD 和 Operator，您需要手动 apply 新的 StarRocksCluster CRD **starrocks.com_starrocksclusters.yaml** 和 **operator.yaml**。

- **[Helm Chart]**

  - 若要升级 Helm Chart，需要执行以下操作：

    1. 使用 **values migration tool** 调整以前的 **values.yaml** 文件的格式为新格式。不同操作系统的值迁移工具可以从 [Assets](https://github.com/StarRocks/starrocks-kubernetes-operator/releases/tag/v1.8.0) 部分下载。您可以通过运行 `migrate-chart-value --help` 命令来获取此工具的帮助信息。[#206](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/206)

         ```Bash
         migrate-chart-value --input values.yaml --target-version v1.8.0 --output ./values-v1.8.0.yaml
         ```

    2. 更新 Helm Chart 仓库。

         ```Bash
         helm repo update
         ```

    3. 执行 `helm upgrade` 命令，使用调整后的 **values.yaml** 文件安装 Chart `kube-starrocks`。

         ```Bash
         helm upgrade <release-name> starrocks/kube-starrocks -f values-v1.8.0.yaml
         ```

  - 将两个子 Chart `operator` 和 `starrocks` 添加到父 Chart kube-starrocks 中。您可以通过指定相应的子 Chart 来安装 StarRocks Operator 或 StarRocks 集群。这样，您可以更灵活地管理 StarRocks 集群，例如部署一个 StarRocks Operator 和多个 StarRocks 集群。

**新增特性**

- **[Helm Chart]** 在一个Kubernetes 集群中部署多个 StarRocks 集群。通过在不同的 namespace 中安装子 Chart `starrocks`，实现在 Kubernetes 集群中部署多个 StarRocks 集群。[#199](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/199)
- **[Helm Chart]** 执行 `helm install` 命令时可以[配置 StarRocks 集群 root 用户的初始密码](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/initialize_root_password_howto.md)。请注意，`helm upgrade` 命令不支持此功能。
- **[Helm Chart]** 与 Datadog 集成。与 Datadog 集成，可以为 StarRocks 集群提供指标和日志。要启用此功能，您需要在 **values.yaml** 文件中配置 datadog 相关字段。使用说明，参见[与 Datadog 集成](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/integration/integration-with-datadog.md)。 [#197](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/197) [#208](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/208)
- **[Operator]** 以非 root 用户身份运行 pod。添加 `runAsNonRoot` 字段以允许 Kubernetes 中的 pod 以非 root 用户运行，从而增强安全性。[#195](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/195)
- **[Operator]** FE proxy。添加 FE proxy 以允许外部客户端和数据导入工具访问 Kubernetes 中的 StarRocks 集群。例如，您可以使用 STREAM LOAD 语法将数据导入至 Kubernetes 中的 StarRocks 集群中。[#211](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/211)

**功能改进**

- 在 StarRocksCluster CRD 中添加 `subpath` 字段。[#212](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/212)
- 将 FE 元数据使用的磁盘容量上限设置得更大。当用于存储 FE 元数据的磁盘可用空间小于该值时，FE 容器停止运行。[#210](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/210)
