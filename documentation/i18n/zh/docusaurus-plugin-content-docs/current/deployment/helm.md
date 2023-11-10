---
displayed_sidebar: "Chinese"
---

# 使用 Helm 部署 StarRocks 集群

[Helm](https://helm.sh/) 是 Kubernetes 的包管理工具。[Helm Chart](https://helm.sh/docs/topics/charts/) 是 Helm 包，包含在 Kubernetes 集群上运行应用程序所需的所有资源定义。本文介绍如何在 Kubernetes 集群上使用 Helm 自动化部署 StarRocks 集群。

## 环境准备

- [创建 Kubernetes 集群](./sr_operator.md#创建-kubernetes-集群)。
- [安装 Helm](https://helm.sh/docs/intro/quickstart/)。

## 部署操作

1. 添加 StarRocks 的 Helm Chart Repo。Helm Chart 包括 StarRocks Operator 和定制资源 StarRocksCluster 的定义。
    1. 添加 Helm Chart Repo。

       ```Bash
       helm repo add starrocks-community https://starrocks.github.io/starrocks-kubernetes-operator
       ```

    2. 更新 Helm Chart Repo 至最新版本。

       ```Bash
       helm repo update
       ```

    3. 查看所添加的 Helm Chart Repo。

       ```Bash
       $ helm search repo starrocks-community
       NAME                                     CHART VERSION   APP VERSION   DESCRIPTION
       starrocks-community/kube-starrocks       1.8.0           3.1-latest    kube-starrocks includes two subcharts, starrock...
       starrocks-community/operator             1.8.0           1.8.0         A Helm chart for StarRocks operator
       starrocks-community/starrocks            1.8.0           3.1-latest    A Helm chart for StarRocks cluster
       ```

2. 您可以使用 Helm Chart 默认的 **[values.yaml](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml)** 来部署 StarRocks Operator 和 StarRocks 集群，也可以新建 YAML 文件并且自定义配置来部署。

    1. 使用默认配置进行部署。
       执行如下命令，部署 StarRocks Operator 和 StarRocks 集群。StarRocks 集群包含一个 FE 和一个 BE。

       ```Bash
       $ helm install starrocks starrocks-community/kube-starrocks
       # 返回如下结果，表示正在部署 StarRocks Operator 和 StarRocks 集群。
       NAME: starrocks
       LAST DEPLOYED: Tue Aug 15 15:12:00 2023
       NAMESPACE: starrocks
       STATUS: deployed
       REVISION: 1
       TEST SUITE: None
       ```

    2. 使用自定义配置进行部署。
        - 创建一个 YAML 文件，例如 **my-values.yaml**，在 YAML 文件中自定义 StarRocks Operator 和 StarRocks
          集群的配置信息。支持配置的参数和说明，请参见 Helm Chart 默认的 **[values.yaml](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml)** 中的注释。
        - 执行如下命令，使用 **my-values.yaml** 中的自定义配置部署 StarRocks Operator 和 StarRocks 集群。

          ```Bash
           helm install -f my-values.yaml starrocks starrocks-community/kube-starrocks
          ```

        部署需要一定时间。期间，您可以根据上述部署命令返回结果中的提示命令查询部署状态，默认的提示命令如下：

       ```Bash
       $ kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
       # 状态显示为 running，则表示已经成功部署。
       NAME             FESTATUS   CNSTATUS   BESTATUS
       kube-starrocks   running               running
       ```

       您也可以执行 `kubectl get pods` 查看部署状态。如果所有 Pod 处于 `Running` 状态且 Pod 内所有容器都 `READY`，则表示已经成功部署。

       ```Bash
       $ kubectl get pods
       NAME                                       READY   STATUS    RESTARTS   AGE
       kube-starrocks-be-0                        1/1     Running   0          2m50s
       kube-starrocks-fe-0                        1/1     Running   0          4m31s
       kube-starrocks-operator-69c5c64595-pc7fv   1/1     Running   0          4m50s
       ```

## 后续步骤

**访问 StarRocks 集群**

支持从 Kubernetes 集群内外访问 StarRocks 集群，具体操作，请参见[访问 StarRocks 集群](./sr_operator.md#访问-starrocks-集群)。

**管理 StarRocks Operator 和 StarRocks 集群**

- 如果需要更新 StarRocks Operator 和 StarRocks 集群的配置，请参见 [Helm Upgrade](https://helm.sh/docs/helm/helm_upgrade/)。
- 如果需要卸载 StarRocks Operator 和 StarRocks 集群，可以执行如下命令：

    ```Bash
    helm uninstall starrocks
    ```

**在 Artifict Hub 上搜索 StarRocks 维护的 Helm Chart**
请参见 [kube-starrocks](https://artifacthub.io/packages/helm/kube-starrocks/kube-starrocks)。
