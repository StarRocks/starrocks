# 使用 StarRocks Operator 在 Kubernetes 部署和管理 CN 【公测中】

自 2.4 版本起，StarRocks 在 FE 、BE 节点基础上，提供了一种新的计算节点（Compute Node，以下简称 CN）。CN 是一种无状态的计算服务，自身不维护数据，可以承担部分 SQL 计算。并且支持基于 Kubernetes 的容器化部署，实现弹性伸缩，支撑数据湖分析等消耗大量计算资源的分析场景。

本文介绍如何使用 StarRocks Operator 在 Kubernetes 上部署 CN 并实现弹性伸缩。

## 基本概念

**Kubernetes**

[Kubernetes](https://kubernetes.io/zh-cn/docs/home/) （以下简称 K8s）是一个开源的容器编排引擎，支持自动化部署、 扩缩和管理容器化应用。

> 容器是可移植、可执行的轻量级镜像，镜像中包含应用以及相关依赖。

**Operator**

[Operator](https://kubernetes.io/zh-cn/docs/concepts/extend-kubernetes/operator/) 是 K8s 提供的一种的扩展软件，允许您通过**[定制资源](https://kubernetes.io/zh-cn/docs/concepts/extend-kubernetes/api-extension/custom-resources/)**来管理应用服务和组件。StarRocks Operator 能够将 StaRocks 所需的计算服务部署到云上，解放复杂的运维管理，实现计算资源自动弹性扩缩容。

**Node**

[Node](https://kubernetes.io/zh-cn/docs/concepts/architecture/nodes/) 是 K8s 集群中资源的实际供给方，是调度 Pod 运行的场所。在生产环境中，一个 K8s 集群通常由多个 Node 组成。

**Pod**

[Pod](https://kubernetes.io/zh-cn/docs/concepts/workloads/pods/) 是可以在 K8s 中创建、管理、可部署的最小计算单元，是一组共享存储网络资源的容器。

**CN**

CN 是 StarRocks 提供的一种无状态计算服务，自身不维护数据，可以承担执行计划中的部分计算。StarRocks operator 使用 HPA 感知 CN 所有实例的 CPU 和内存负载，并且根据您配置的策略自动扩缩 CN。

## 工作原理

![image](../assets/9.1.png)

**StarRocks Operator、CN 和 StarRocks 集群的交互方式**

StarRocks Operator 通过 FE 的 IP 地址和 FE 的查询端口连接 FE ，将 CN 加入至 StarRocks 集群中。

FE 按照数据分布情况和算子类型将执行计划中的计算任务分配给 CN，并告诉 CN 数据的来源。CN 从数据源中获取数据进行计算，结束后将结果返回给FE。

**自动扩缩容策略**

在部署 computenodegroup 资源时需要指定自动扩缩容策略，StarRocks operator 会根据策略创建一个 HPA 资源， HPA 会根据 CN 的 CPU 和 内存等指标自动扩缩 CN 节点。

## 环境准备

- 部署 StarRocks 集群。部署方式请参考[部署 StarRocks](https://docs.starrocks.com/zh-cn/latest/quick_start/Deploy)。

- 各个节点的网络互通： FE 能够直接访问 K8s 集群中的 pod，并且 CN 能够访问 BE。

- 部署 [Kubernetes 集群](https://kubernetes.io/zh-cn/docs/setup/production-environment/tools/kubeadm/)。

  > 如果您想要快速体验本特性，可以使用 [Minikube](https://kubernetes.io/zh-cn/docs/tutorials/kubernetes-basics/create-cluster/cluster-intro/) 创建单节点 Kubernetes 集群。

- 在 StarRocks Operator 所在机器上，安装 [GO 语言开发环境](https://go.dev/doc/install)。

## 制作 Docker 镜像

您需要制作 Docker 镜像，包括 StarRocks Operator、CN、CN 辅助服务的镜像，并且将镜像推送至远端仓库。部署 StarRocks Operator、CN 时，会从远端仓库中拉取镜像。

### 制作 StarRocks  Operator 镜像

1. 下载 StarRocks Operator 代码，并保存至目录 `$your_code_path/``starrocks-kubernetes-operator`。

    ```Bash
    cd $your_code_path
    git clone https://github.com/StarRocks/starrocks-kubernetes-operator
    ```

2. 进入 StarRocks Operator 代码目录。

    ```Bash
    cd starrocks-kubernetes-operator
    ```

3. 制作 StarRocks  Operator 镜像。

    ```Bash
    make docker IMG="starrocks-kubernetes-operator:v1.0"
    ```

4. 执行 `docker login`，按照提示输入账号和密码，登录远端 Docker 仓库  [Docker Hub](https://hub.docker.com)。

    > 说明：您需要提前在 [Docker Hub](https://hub.docker.com) 中注册账号、创建 Docker 仓库。

5. 标记 StarRocks Operator 镜像，并推送至远端 Docker 仓库。

    ```Bash
    docker tag $operator_image_id $account/repo:tag
    docker push $account/repo:tag
    ```

说明：

- `dockerImageId`：StarRocks Operator 镜像 ID，可以执行 `docker images` 进行查看。

- `account/repo:tag`：StarRocks Operator 镜像标签，例如`starrocks/sr-cn-test:operator`。其中，`account` 为 Docker Hub 中注册的账号；`repo` 为 Docker 镜像仓库；`tag`为自定义的 StarRocks Operator 镜像标签。

### 制作 CN 镜像

1. 下载 StarRocks 的 Github 仓库代码。

    ```Bash
    git clone https://github.com/StarRocks/starrocks
    ```

2. 进入 **docker** 目录。

    ```Bash
    cd $your_path/starrocks/docker
    ```

3. 编译 StarRocks 并制作 CN 镜像。

    ```Plaintext
    ./build.sh -b branch-2.4
    ```

4. 标记 CN 镜像，并推送至远端 Docker 仓库。

    ```Bash
    docker tag $cn_Image_Id $account/repo:tag
    docker push $account/repo:tag
    ```

### 制作 CN  辅助服务镜像

CN 辅助服务是指 **component** 目录下的register、offline，会将 CN 注册到至 FE 或者从 FE 中摘除。

1. 进入 **starrocks-kubernetes-operator****/components** 目录。

    ```Bash
    cd $your_code_path/starrocks-kubernetes-operator/components
    ```

2. 制作 CN 辅助服务镜像，并推送至远端仓库。

    ```Bash
    # build image
    make docker IMG="computenodegroup:v1.0"
    # push image to docker hub
    make push IMG="computenodegroup:v1.0"
    ```

## 部署 StarRocks Operator

1. 进入 **starrocks-kubernetes-operator****/deploy** 目录。

    ```Bash
    cd $your_code_path/starrocks-kubernetes-operator/deploy
    ```

2. 修改 **manager.yaml** 的 `image`为制作的 StarRocks Operator 镜像标签。例如 `starrocks/sr-cn-test:operator` 。
    ![image](../assets/9.2.png)

3. 执行如下命令，部署 StarRocks Operator。

    ```Bash
    kubectl apply -f starrocks.com_computenodegroups.yaml
    kubectl apply -f namespace.yaml
    kubectl apply -f leader_election_role.yaml
    kubectl apply -f role.yaml
    kubectl apply -f role_binding.yaml
    kubectl apply -f leader_election_role_binding.yaml
    kubectl apply -f service_account.yaml
    kubectl apply -f manager.yaml 
    ```

4. 执行 `kubectl get pod -n starrocks` 查看 pod 状态。如果返回结果中 `STATUS` 显示`Running`，则表示 pod 正在运行。

    ```Bash
    kubectl get pod -n starrocks
    NAMESPACE     NAME                                                READY   STATUS             RESTARTS        AGE
    starrocks     cn-controller-manager-69598d4b48-6qj2p              1/1     Running            0               13h
    ```

## 在 K8s 中部署 CN

1. 进入 **starrocks-kubernetes-operator/examples/cn** 目录。

    ```Bash
    cd $your_code_path/starrocks-kubernetes-operator/examples/cn
    ```

2. 修改 **cn.yaml**。
   1. 修改 `cnImage` 为推送到远端仓库的 CN 镜像文件标签。例如:  `starrocks/sr-cn-test:v3`
   2. 修改 `componentsImage` 为推送到远端仓库的 CN Group 镜像文件标签。例如:  `starrocks/computenodegroup:v1.0`
   3. 修改 `<fe_ip>:<fe_query_port>` 为任意一个 FE 节点 IP 地址和 `query_port` 端口号（默认为 `9030`）。

      ```Bash
          feInfo:
                accountSecret: test-secret # secret to configure   FE   account
              addresses: #   FE   addresses
                - <fe_ip>:<fe_query_port>
        ```

   4. 增加 `command` 配置选项，并且修改路径为 CN Group 镜像中 **start_cn.shell** 的绝对路径。
      - ![image](../assets/9.3.png)

3. 部署 CN。

    ```Bash
    cd examples/cn
    kubectl apply -f fe-account.yaml
    kubectl apply -f cn-config.yaml
    kubectl apply -f cn.yaml
    ```

4. 检查 CN 运行状态。

    ```Bash
    $ kubectl get pod -n starrocks # 命名空间默认为 starrocks
    NAMESPACE     NAME                                                READY   STATUS             RESTARTS        AGE
    starrocks     cn-controller-manager-69598d4b48-6qj2p              1/1     Running            0               13h
    starrocks     computenodegroup-sample-8-4-21-45-5dcb56ff5-4l522   2/2     Running            0               12m
    starrocks     computenodegroup-sample-8-4-21-45-5dcb56ff5-cfvmj   2/2     Running            0               4m29s
    starrocks     computenodegroup-sample-8-4-21-45-5dcb56ff5-lz5s2   2/2     Running            0               4m23s
    starrocks     computenodegroup-sample-8-4-21-45-5dcb56ff5-s7dwz   2/2     Running            0               12m
    ```

CN 部署成功后， StarRocks Operator 自动调用 **cn.yaml** 文件中配置的 FE IP 和查询端口号，将 CN 加至 StarRocks 集群中。

## 配置自动水平扩缩容策略

1. 如果需要配置 CN 自动扩缩策略，则可以修改 **cn.yaml** 配置文件`$your_code_path/starrocks-kubernetes-operator/examples/cn/cn.yaml`。
   1. 例如，您需要基于 K8s 中 CN 的内存和 CPU 使用率实现弹性伸缩，则需要配置内存和 CPU 平均使用率为资源指标，触发弹性伸缩的阈值。弹性伸缩上限和下限即 pod 副本数量或者 CN 数量的上限和下限。

   2. Kubernetes 还支持使用 `behavior`，根据业务场景定制扩缩容行为，实现快速扩容，缓慢缩容，禁用缩容等。

    ```Bash
    autoScalingPolicy: # auto-scaling policy of CN cluster
          maxReplicas: 10 # CN 数量的上限 10
          minReplicas: 1 # CN 数量的下限 1
          hpaPolicy:
            metrics: # 资源指标
              - type: Resource
                resource: 
                  name: memory # 资源指标为内存
                  target:
                    averageUtilization: 30 # 触发水平扩缩容的阈值为30%。 K8s 集群中 CN 内存使用率超过 30% 时，增加 CN 数量进行扩容，低于 30% 时，减少 CN 数量进行缩容。
                    type: Utilization
              - type: Resource
                resource: # 触发水平扩缩容的阈值为 60%。K8s 集群中 CN CPU 内存使用率超过 60% 时，增加 CN 数量进行扩容，低于 60% 时，减少 CN 数量进行缩容。
                  name: cpu
                  target:
                    averageUtilization: 60
                    type: Utilization
            behavior: # 根据业务场景定制扩缩容行为，实现快速扩容，缓慢缩容，禁用缩容等。 
              scaleUp:
                policies:
                  - type: Pods
                    value: 1
                    periodSeconds: 10
              scaleDown:
                selectPolicy: Disabled
    ```

    部分配置说明如下：

    > 自动水平扩缩容的更多配置，请参考 [Pod 水平自动扩缩](https://kubernetes.io/zh-cn/docs/tasks/run-application/horizontal-pod-autoscale/)。

    - 水平扩缩时 CN 数量的上限和下限。

        ```Bash
        # CN 数量的上限 10
        maxReplicas: 10
        # CN 数量的下限 1
        minReplicas: 1
       ```

    - 触发水平扩缩的阈值。

        ```Bash
        # 触发水平扩缩容的阈值，例如资源指标为 K8s 集群中 CN CPU 使用率。当 CPU 使用率超过 60% 时，增加 CN 数量进行扩容，低于 60% 时，减少 CN 数量进行缩容。
        - type: Resource
          resource:
            name: cpu
            target:
              averageUtilization: 60
        ```

2. 生效自动扩缩策略。

    ```Plaintext
    kubectl apply -f cn/cn.yaml
    ```

## 常见问题

### 在 K8s 中部署 CN 异常

  执行 `Kubectl get po -A` 检查 pod 状态。

- 问题：如果 `reason` 显示 `unhealthy`，则表示 HTTP 健康检查失败。
  ![image](../assets/9.4.png)

- 解决方式：参考[部署CN ](/部署 CN)，检查 **cn.yaml** 文件中的 FE 节点 IP 地址和 FE 的查询端口号。

  ![image](../assets/9.5.png)

- 问题：如果 `Message` 显示`exec: "be/bin/start_cn.sh": stat be/bin/start_cn.sh: no such file or directory`，表示获取启动脚本失败。

    ```Plain
    Events:
    Type     Reason     Age                    From               Message
      ----     ------     ----                   ----               -------
      Normal   Scheduled  5m53s                  default-scheduler  Successfully assigned starrocks/computenodegroup-sample-5979687fd-qw28w to ip-172-31-44-58
      Normal   Pulling    5m51s                  kubelet            Pulling image "adzfolc/computenodegroup:v1.1"
      Normal   Started    5m48s                  kubelet            Started container register
      Normal   Pulled     5m48s                  kubelet            Successfully pulled image "adzfolc/computenodegroup:v1.1" in 2.865218014s
      Normal   Created    5m48s                  kubelet            Created container register
      Normal   Created    5m3s (x4 over 5m51s)   kubelet            Created container cn-container
      Warning  Failed     5m3s (x4 over 5m51s)   kubelet            Error: failed to start container "cn-container": Error response from daemon: OCI runtime create failed: container_linux.go:380: starting container process caused: exec: "be/bin/start_cn.sh": stat be/bin/start_cn.sh: no such file or directory: unknown
      Normal   Pulled     4m15s (x5 over 5m51s)  kubelet            Container image "adzfolc/sr-cn-test:v3" already present on machine
      Warning  BackOff    41s (x27 over 5m46s)   kubelet            Back-off restarting failed container
    ```

- 解决方式：参考[在 K8s 中部署 CN](#在-k8s-中部署-cn)，检查 **cn.yaml** 文件中的 **start_cn.shell** 的路径是否正确。
