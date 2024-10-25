---
displayed_sidebar: docs
---

# Deploy StarRocks with Helm

[Helm](https://helm.sh/) is a package manager for Kubernetes. A [Helm Chart](https://helm.sh/docs/topics/charts/) is a Helm package and contains all of the resource definitions necessary to run an application on a Kubernetes cluster. This topic describes how to use Helm to automatically deploy a StarRocks cluster on a Kubernetes cluster.

## Before you begin

- [Create a Kubernetes cluster](./sr_operator.md#create-kubernetes-cluster).
- [Install Helm](https://helm.sh/docs/intro/quickstart/).

## Procedure

1. Add the Helm Chart Repo for StarRocks. The Helm Chart contains the definitions of the StarRocks Operator and the custom resource StarRocksCluster.
   1. Add the Helm Chart Repo.

      ```Bash
      helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator
      ```

   2. Update the Helm Chart Repo to the latest version.

      ```Bash
      helm repo update
      ```

   3. View the Helm Chart Repo that you added.

      ```Bash
      $ helm search repo starrocks
      NAME                                    CHART VERSION    APP VERSION  DESCRIPTION
      starrocks/kube-starrocks      1.8.0            3.1-latest   kube-starrocks includes two subcharts, starrock...
      starrocks/operator            1.8.0            1.8.0        A Helm chart for StarRocks operator
      starrocks/starrocks           1.8.0            3.1-latest   A Helm chart for StarRocks cluster
      ```

2. Use the default **[values.yaml](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml)** of the Helm Chart to deploy the StarRocks Operator and StarRocks cluster, or create a YAML file to customize your deployment configurations.
   1. Deployment with default configurations

      Run the following command to deploy the StarRocks Operator and the StarRocks cluster which consists of one FE and one BE:
   > Tip
   >
   > The default `values.yaml` is configured to deploy:
   > - The operator pod with 1/2CPU and 0.8GB RAM
   > - One FE with 4GB RAM, 4 cores, and 15Gi disk
   > - One BE with 4GB RAM, 4 cores, and 1Ti disk
   >
   > If you do not have these resources available in your Kubernetes cluster then skip to the **Deployment with custom configurations** section and adjust the resources.

      ```Bash
      $ helm install starrocks starrocks/kube-starrocks
      # If the following result is returned, the StarRocks Operator and StarRocks cluster are being deployed.
      NAME: starrocks
      LAST DEPLOYED: Tue Aug 15 15:12:00 2023
      NAMESPACE: starrocks
      STATUS: deployed
      REVISION: 1
      TEST SUITE: None
      ```

3. Deployment with custom configurations
   - Create a YAML file, for example, **my-values.yaml**, and customize the configurations for the StarRocks Operator and StarRocks cluster in the YAML file. For the supported parameters and descriptions, see the comments in the default **[values.yaml](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml)** of the Helm Chart.
   - Run the following command to deploy the StarRocks Operator and StarRocks cluster with the custom configurations in **my-values.yaml**.

     ```bash
     helm install -f my-values.yaml starrocks starrocks/kube-starrocks
     ```

    Deployment takes a while. During this period, you can check the deployment status with:

    ```bash
    kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
    ```
    If the following result is returned, the deployment has been successfully completed.
   
    ```bash
    NAME             PHASE     FESTATUS   BESTATUS   CNSTATUS   FEPROXYSTATUS
    kube-starrocks   running   running    running
    ```

    You can also run `kubectl get pods` to check the deployment status. If all Pods are in the `Running` state and all containers within the Pods are `READY`, the deployment has been successfully completed.

    ```bash
    kubectl get pods
    ```

    ```bash
    NAME                                       READY   STATUS    RESTARTS   AGE
    kube-starrocks-be-0                        1/1     Running   0          2m50s
    kube-starrocks-fe-0                        1/1     Running   0          4m31s
    kube-starrocks-operator-69c5c64595-pc7fv   1/1     Running   0          4m50s
    ```

## Next steps

- Access StarRocks cluster

  You can access the StarRocks cluster from inside and outside the Kubernetes cluster. For detailed instructions, see [Access StarRocks Cluster](./sr_operator.md#access-starrocks-cluster).

- Manage StarRocks operator and StarRocks cluster

  - If you need to update the configurations of the StarRocks operator and StarRocks cluster, see [Helm Upgrade](https://helm.sh/docs/helm/helm_upgrade/).
  - If you need to uninstall the StarRocks Operator and StarRocks cluster, run the following command:

    ```bash
    helm uninstall starrocks
    ```

## More information

- The address of the GitHub repository: [starrocks-kubernetes-operator and kube-starrocks Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator).

- The docs in the GitHub repository provide more information, for example:

  - If you need to manage objects like the StarRocks cluster via the Kubernetes API, see [API reference](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/api.md).

  - If you need to mount persistent volumes to FE and BE pods to store FE metadata and logs, as well as BE data and logs, see [Mount Persistent Volumes by Helm Chart](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/mount_persistent_volume_howto.md#2-mounting-persistent-volumes-by-helm-chart).

    :::danger

    If persistent volumes are not mounted, the StarRocks Operator will use emptyDir to store FE metadata and logs, as well as BE data and logs. When containers restart, data will be lost.

    :::

  - If you need to set the root user password:

    - Manually set the root user's password after deploying the StarRocks cluster, see [Change root user password HOWTO](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/change_root_password_howto.md).

    - Automatically set the root user's password when deploying the StarRocks cluster, see [Initialize root user password](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/initialize_root_password_howto.md).

- How to resolve the following error that occurs after a CREATE TABLE statement is executed in a StarRocks shared-data cluster.

  - **Error message**

      ```plaintext
      ERROR 1064 (HY000): Table replication num should be less than or equal to the number of available BE nodes. You can change this default by setting the replication_num table properties. Current alive backend is [10001]. , table=orders1, default_replication_num=3
      ```

  - **Solution**

       This may be because only one BE exists in that StarRocks shared-data cluster, which supports only one replica. However, the default number of replicas is 3. You can modify the number of replicas to 1 in PROPERTIES, such as, `PROPERTIES( "replication_num" = "1" )`.

- The address of Helm Chart maintained by StarRocks on Artifact Hub: [kube-starrocks](https://artifacthub.io/packages/helm/kube-starrocks/kube-starrocks).
