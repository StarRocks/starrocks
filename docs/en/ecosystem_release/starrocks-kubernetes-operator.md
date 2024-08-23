---
displayed_sidebar: docs
---

# Releases of Kubernetes Operator for StarRocks

## Notifications

The Operator provided by StarRocks is used to deploy StarRocks clusters in the Kubernetes environment. The StarRocks cluster components include FE, BE, and CN.

**User guide:** You can use the following methods to deploy StarRocks clusters on Kubernetes:

- [Directly use StarRocks CRD to deploy StarRocks clusters](https://docs.starrocks.io/docs/deployment/sr_operator/)
- [Deploying both the Operator and StarRocks clusters by using the Helm Chart](https://docs.starrocks.io/zh/docs/deployment/helm/)

**Source codes:**

[starrocks-kubernetes-operator and kube-starrocks Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator)

**Download URL of the resources:**

- **URL prefix**

    `https://github.com/StarRocks/starrocks-kubernetes-operator/releases/download/v${operator_version}/${resource_name}`

- **Resource name**

  - StarRocksCluster CRD: `starrocks.com_starrocksclusters.yaml`
  - Default configuration file for StarRocks Operator: `operator.yaml`
  - Helm Chart, including `kube-starrocks` Chart `kube-starrocks-${chart_version}.tgz`. The `kube-starrocks` Chart is divided into two subcharts: `starrocks` Chart `starrocks-${chart_version}.tgz` and `operator` Chart `operator-${chart_version}.tgz`.

For example, the download URL for kube-starrocks chart v1.8.6 is:

`https://github.com/StarRocks/starrocks-kubernetes-operator/releases/download/v1.8.6/kube-starrocks-1.8.6.tgz`

**Version requirements**

- Kubernetes: 1.18 or later
- Go: 1.19 or later

## Release notes

### 1.9

#### 1.9.1

**Improvements**

- **[Helm Chart]** The operator will not create PersistentVolumeClaim (PVC) for log storage when `logStorageSize` is set to `0`.  [#398](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/398)
- **[Operator]** The operator can detect whether the values of `mountPath` and `name` in `storageVolumes` are duplicated. An error is returned when duplicate values exist. [#388](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/388)
- **[Operator]** The number of FE nodes cannot be scaled down to 1. [#394](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/394)
- **[Operator]** The values of the `feEnvVars`, `beEnvVars` and `cnEnvVars` fields in multiple values YAML files can be merged. [#396](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/396)
- **[Operator]** Add `spec.containers.securityContext.capabilities` in the StarRocksCluster CRD to customize the Linux capabilities of containers. [#404](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/404)

**Bug Fixes**

Fixed the following issues:

- **[Operator]** The `annotations` field in `service` can be updated. [#402](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/402) [#399](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/399)
- **[Operator]** The modification of statefulset and deployment is patched instead of updated. It solves the problem that upgrading CN will cause all CN pods to be terminated and restarted when CN and HPA are enabled. [#397](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/397)
- **[Operator]** The modification of service object is patched instead of updated. It prevents the operator from overwriting the modification, for example, when a Kubernetes Cloud provider is used and the service object is modified by that Kubernetes Cloud provider. [#387](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/387)

#### 1.9.0

**New features**

- Add StarRocksWarehouse CRD to support StarRocks Warehouse. Note that StarRocks Warehouse is currently a feature of the StarRocks Enterprise Edition. [#323](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/323)

**Enhancements**

- Add the `status.reason` field in StarRocksCluster CRD. When the apply operation of a subcontroller fails during cluster deployment, you can execute `kubectl get starrockscluster <name_of_the_starrocks_cluster_object> -oyaml`, and view error logs shown in `status.reason` field in the returned result. [#359](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/359)
- An empty directory can be mounted in the `storageVolumes` field. [#324](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/324)

**Bug Fixes**

Fixed the following issues:

- The status of the StarRocks cluster was inconsistent with the status of the cluster's FE, BE and CN components. [#380](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/380)
- The HPA resource is not deleted when `autoScalingPolicy` is deleted. [#379](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/379)
- The HPA resource is not removed when `starRocksCnSpec` is deleted. [#357](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/357)

### 1.8

#### 1.8.8

**Bugfixes**

- **[Operator]** The operator no longer annotates on search service (which is an internal service) when annotations are added by using `StarRocksFeSpec.service`, `StarRocksBeSpec.service`, and `StarRocksCnSpec.service`.  [#370](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/370)

#### 1.8.7

**Enhancements**

- Add the `livenessProbeFailureSeconds` and `readinessProbeFailureSeconds` fields in StarRocksCluster CRD. When StarRocks is under heavy workload and the time of liveness and readiness probes still use default values, the liveness and readiness probes may fail and cause the containers to restart. In this case, you can add larger values to these two fields. [#309](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/309)

#### 1.8.6

**Bug Fixes**

Fixed the following issue:

- An error  `sendfile() failed (32: Broken pipe) while sending request to upstream` is returned during a Stream Load job. After Nginx sends the request body to FE, the FE then redirects the request to the BE. At this point, the data cached in Nginx may already be lost. [#303](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/303)

**Doc**

- [Load data from outside the Kubernetes network to StarRocks through FE proxy](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/load_data_using_stream_load_howto.md)
- [Update the root user's password using Helm](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/change_root_password_howto.md)

#### 1.8.5

**Improvements**

- **[Helm Chart] The `annotations` and `labels` can be customized for the service account of the operator**: The Operator creates a service account named `starrocks` by default, and users can customize the annotations and labels for the service account `starrocks` of the operator by specifying the `annotations` and `labels` fields in `serviceAccount` in **values.yaml**. The `operator.global.rbac.serviceAccountName` field is deprecated. [#291](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/291)
- **[Operator] FE service supports explicit protocol selection for Istio**: When Istio is installed in the Kubernetes environment, Istio needs to determine the protocol of the traffic from the StarRocks cluster, in order to provide additional functionality such as routing and rich metrics. So FE service explicitly defines its protocol as MySQL in the `appProtocol` field. This improvement is particularly important because the MySQL protocol is a server-first protocol that is incompatible with automatic protocol detection and sometimes may incur connection failures. [#288](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/288)

**Bug Fixes**

Fixed the following issue:

- **[Helm Chart]** The root user's password in StarRocks may not be initialized successfully when `starrocks.initPassword.enabled` is true and the value of `starrocks.starrocksCluster.name` is specified. It is caused by the wrong FE service domain name used by the initpwd pod to connect FE service. More specifically, in this scenario, FE service domain name uses the value specified in `starrocks.starrocksCluster.name`, while the initpwd pod still uses the value of the `starrocks.nameOverride` field to form the FE service domain name. ([#292](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/292))

**Upgrade notes**

- **[Helm Chart]** When the value specified in `starrocks.starrocksCluster.name` is different from the value of `starrocks.nameOverride`, the old configmaps for FE, BE, and CN will be deleted. New configmaps with new names for the FE, BE, and CN will be created. **This may result in the restart of the FE, BE, and CN pods.**

#### 1.8.4

**Features**

- **[Helm Chart]** The metrics of StarRocks clusters can be monitored by using the Prometheus and ServiceMonitor CR. For the user guide, see [Integration with Prometheus and Grafana](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/integration/integration-prometheus-grafana.md). [#284](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/284)
- **[Helm Chart]** Add the `storagespec` and more fields in `starrocksCnSpec` in **values.yaml** to configure the log volume for CN nodes in a StarRocks Cluster. [#280](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/280)
- Add the `terminationGracePeriodSeconds` in the StarRocksCluster CRD to configure how long to wait before forcefully terminating a pod when a StarRocksCluster resource is being deleted or updated. [#283](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/283)
- Add the `startupProbeFailureSeconds` field in the StarRocksCluster CRD to configure the startup probe failure threshold for the pods in the StarRocksCluster resource. [#271](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/271)

**Bug Fixes**

Fixed the following issue:

- The FE Proxy cannot handle STREAM LOAD requests correctly when multiple FE pods exist in the StarRocks cluster. [#269](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/269)

**Doc**

- [Add a quick start on how to deploy a local StarRocks cluster](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/local_installation_how_to.md).
- Add more user guides on how to deploy a StarRocks cluster with different configurations. For example, how to [deploy a StarRocks cluster with all supported features](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/examples/starrocks/deploy_a_starrocks_cluster_with_all_features.yaml). For more user guides, see [docs](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/examples/starrocks).
- Add more user guides on how to manage the StarRocks cluster. For example, how to configure [logging and related fields](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/logging_and_related_configurations_howto.md) and [mount external configmaps or secrets](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/mount_external_configmaps_or_secrets_howto.md). For more user guides, see [docs](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/doc).

#### 1.8.3

**Upgrade notes**

- **[Helm Chart]** Add `JAVA_OPTS_FOR_JDK_11` to the default **fe.conf** file. When the default **fe.conf** file is used and the helm chart is upgraded to v1.8.3, **FE pods may restart**. [#257](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/257)

**Features**

- **[Helm Chart]** Add the `watchNamespace` field to specify the one and only namespace that the operator needs to watch. Otherwise, the operator watches all namespaces in the Kubernetes cluster. In most cases, you do not need to use this feature. You can use this feature when the Kubernetes cluster manages too many nodes, the operator watches all namespaces and consumes too many memory resources. [#261](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/261)
- **[Helm Chart]** Add the `Ports` field in `starrocksFeProxySpec` in the **values.yaml** file to allow users to specify the NodePort of FE Proxy service. [#258](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/258)

**Improvements**

- The value of the `proxy_read_timeout` parameter is changed in the **nginx.conf** file to 600s from 60s, in order to avoid timeout.

#### 1.8.2

**Improvements**

- Increase the maximum memory usage allowed for the operator pods to avoid OOM. [#254](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/254)

#### 1.8.1

**Features**

- Support using [the subpath field in the configMaps and secrets](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/mount_external_configmaps_or_secrets_howto.md#3-mount-configmaps-to-a-subpath-by-helm-chart), allowing users to mount specific files or directories from these resources. [#249](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/249)
- Add the `ports` field in the StarRocks cluster CRD to allow users to customize the ports of the services. [#244](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/244)

**Improvements**

- Remove the related Kubernetes resources when the `BeSpec` or `CnSpec` of the StarRocks cluster is deleted, ensuring a clean and consistent state of the cluster. [#245](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/245)

#### 1.8.0

**Upgrade notes and behavior changes**

- **[Operator]** To upgrade the StarRocksCluster CRD and operator you need to manually apply the new StarRocksCluster CRD **starrocks.com_starrocksclusters.yaml** and **operator.yaml.**

- **[Helm Chart]**

  - To upgrade the Helm Chart, you need to perform the following:

    1. Use the **values migration tool** to adjust the format of the previous **values.yaml** file to the new format. The values migration tool for different operating systems can be downloaded from the [Assets](https://github.com/StarRocks/starrocks-kubernetes-operator/releases/tag/v1.8.0) section. You can get help information of this tool by running the `migrate-chart-value --help` command. [#206](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/206)

       ```Bash
       migrate-chart-value --input values.yaml --target-version v1.8.0 --output ./values-v1.8.0.yaml
       ```

    2. Update the Helm Chart repo.

       ```Bash
       helm repo update
       ```

    3. Execute the `helm upgrade` command to apply the adjusted **values.yaml** file to the StarRocks helm chart kube-starrocks.

       ```Bash
       helm upgrade <release-name> starrocks/kube-starrocks -f values-v1.8.0.yaml
       ```

  - Two subcharts, [operator](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/helm-charts/charts/kube-starrocks/charts/operator) and [starrocks](https://github.com/StarRocks/starrocks-kubernetes-operator/tree/main/helm-charts/charts/kube-starrocks/charts/starrocks), are added into the kube-starrocks helm chart. You can choose to install StarRocks operator or StarRocks cluster respectively by specifying the corresponding subchart. This way, you can manage StarRocks clusters more flexibly, such as deploying one StarRocks operator and multiple StarRocks clusters.

**Features**

- **[Helm Chart] Multiple StarRocks clusters in a Kubernetes cluster**. Support deploying multiple StarRocks clusters in different namespaces in a Kubernetes cluster by installing the `starrocks` Helm subchart.  [#199](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/199)
- **[Helm Chart]** Support to [configure the initial password of StarRocks cluster's root users](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/initialize_root_password_howto.md) when executing the `helm install` command. Note that, the `helm upgrade` command does not support this feature.
- **[Helm Chart] Integration with Datadog:** Integrated with Datadog to collect StarRocks clusters' metrics and logs. To enable this feature, you need to configure the Datadog related fields in the **values.yaml** file. For the detailed user guide, see [Integration with Datadog](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/integration/integration-with-datadog.md). [#197](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/197) [#208](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/208)
- **[Operator] Run pods as a non-root user**. Add the runAsNonRoot field to allow pods to run as non-root users, which can enhance security. [#195](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/195)
- **[Operator] FE proxy.** Add the FE proxy to allow external clients and data load tools that support Stream Load protocol to access StarRocks clusters in Kubernetes. This way, you can use the load job based on Stream Load to load data into StarRocks clusters in Kubernetes. [#211](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/211)

**Improvements**

- Add the `subpath` field in StarRocksCluster CRD. [#212](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/212)
- Increase the disk size allowed for the FE metadata. The FE container stops running when the available disk space that can be provisioned to store the FE metadata is less than that default value. [#210](https://github.com/StarRocks/starrocks-kubernetes-operator/pull/210)
