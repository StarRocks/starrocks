# Deploy StarRocks with Helm

[Helm](https://helm.sh/) is a package manager for Kubernetes. A [Helm Chart](https://helm.sh/docs/topics/charts/) is a Helm package and contains all of the resource definitions necessary to run an application on a Kubernetes cluster. This topic describes how to use Helm to automatically deploy a StarRocks cluster on a Kubernetes cluster.

## Before you begin

- [Create a Kubernetes cluster](../sr_operator#create-kubernetes-cluster).
- [Install Helm](https://helm.sh/docs/intro/quickstart/).

## Procedure

> **NOTICE**
>
> It is not recommended to use Helm to deploy a StarRocks cluster in a production environment.

1. Add the Helm Chart Repo for StarRocks. The Helm Chart contains the definitions of the StarRocks Operator and the custom resource StarRocksCluster.
   1. Add the Helm Chart Repo.

      ```Bash
      helm repo add starrocks-community https://starrocks.github.io/helm-charts
      ```

      > **NOTE**
      >
      > You can also add the [Helm Chart Repo from Artifact Hub](). For detailed information, see https://artifacthub.io/packages/helm/fslstarrocks/kube-starrocks

   2. Update the Helm Chart Repo to the latest version.

      ```Bash
      helm repo update
      ```

   3. View the Helm Chart Repo that you added.

      ```Bash
      $ helm search repo starrocks-community
      NAME                                      CHART VERSION        APP VERSION        DESCRIPTION                                       
      starrocks-community/kube-starrocks        1.6.1                2.5.4              kube-starrocks collects Kubernetes manifests, s...
      ```

2. Use the default **[values.yaml](https://github.com/StarRocks/helm-charts/blob/main/charts/kube-starrocks/values.yaml)** of the Helm Chart to deploy the StarRocks Operator and StarRocks cluster, or create a YAML file to customize your deployment configurations.
   1. Deployment with default configurations

      Run the following command to deploy the StarRocks Operator and the StarRocks cluster which consists of one FE and one BE:

      ```Bash
      $ helm install starrocks starrocks-community/kube-starrocks
      # If the following result is returned, the StarRocks Operator and StarRocks cluster are being deployed.
      NAME: starrocks
      LAST DEPLOYED: Tue Apr 11 12:11:12 2023
      NAMESPACE: default
      STATUS: deployed
      REVISION: 1
      NOTES:
      1. Get the application URL by running these commands:
        
      kube-starrocks has been installed, Check its status by running:
      kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
        
      Visit https://github.com/starrocks for instructions on how to create & configure.
      ```

   2. Deployment with custom configurations
      - Create a YAML file, for example, **my-values.yaml**, and customize the configurations for the StarRocks Operator and StarRocks cluster in the YAML file. For the supported parameters and descriptions, see the comments in the default **[values.yaml](https://github.com/StarRocks/helm-charts/blob/main/charts/kube-starrocks/values.yaml)** of the Helm Chart.
      - Run the following command to deploy the StarRocks Operator and StarRocks cluster with the custom configurations in **my-values.yaml**.

        ```Bash
        helm install -f my-values.yaml starrocks starrocks-community/kube-starrocks
        ```

    Deployment takes a while. During this period, you can check the deployment status by using the prompt command in the returned result of the deployment command above. The default prompt command is as follows:

    ```Bash
    $ kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
    # If the following result is returned, the deployment has been successfully completed.
    NAME             FESTATUS   CNSTATUS   BESTATUS
    kube-starrocks   running               running
    ```

    You can also run `kubectl get pods` to check the deployment status. If all Pods are in the `Running` state and all containers within the Pods are `READY`, the deployment has been successfully completed.

    ```Bash
    $ kubectl get pods
    NAME                                       READY   STATUS    RESTARTS   AGE
    kube-starrocks-be-0                        1/1     Running   0          2m50s
    kube-starrocks-fe-0                        1/1     Running   0          4m31s
    kube-starrocks-operator-69c5c64595-pc7fv   1/1     Running   0          4m50s
    ```

## Next steps

**Access StarRocks cluster**

You can access the StarRocks cluster from inside and outside the Kubernetes cluster. For detailed instructions, see [Access StarRocks Cluster](https://chat.openai.com/sr_operator#accessing-the-starrocks-cluster).

> **NOTICE**
>
> The StarRocks cluster is located in the namespace `default`.

**Manage StarRocks operator and StarRocks cluster**

- If you need to update the configurations of the StarRocks operator and StarRocks cluster, see [Helm Upgrade](https://helm.sh/docs/helm/helm_upgrade/).
- If you need to uninstall the StarRocks Operator and StarRocks cluster, run the following command:

  ```bash
  helm uninstall starrocks
  ```

**Search Helm Chart maintained by StarRocks on Artifact Hub**

See [kube-starrocks](https://artifacthub.io/packages/helm/fslstarrocks/kube-starrocks).
