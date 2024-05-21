# Deploy a Warehouse

A warehouse in CelerData Cloud Serverless is an independent group of compute nodes that can provide with you the required compute resources (CPU, memory, and temporary storage) for SQL execution. Each warehouse can be suspended, resumed, and scaled on demand.

:::note

Warehouses are used by StarRocks clusters running in shared-data mode. You cannot add a Warehouse to a shared-nothing StarRocks cluster.

:::

This topic describes how to create a warehouse and assign it to a user.

## Prerequisites

- You have a StarRocks shared-data cluster deployed in your Kubernetes cluster.
- You have the StarRocks Operator installed in your Kubernetes cluster.

## Deploy a Warehouse

1. Prepare a `values.yaml file for the Warehouse chart.

```yaml
# wh1-values.yaml
spec:
  # Make sure the StarRocks cluster exists in the same namespace.
  # You can check it by running `kubectl get starrocksclusters.starrocks.com`.
  starRocksClusterName: kube-starrocks
  replicas: 1
  image:
    repository: us-west1-docker.pkg.dev/phrasal-verve-350013/celerdata/cn-ubuntu
    tag: "3.2.6-ee"
  resources:
    limits:
      cpu: 8
      memory: 8Gi
    requests:
      cpu: 8
      memory: 8Gi
```

2. Deploy the Warehouse with the following command:

```console
helm install wh1 starrocks-community/warehouse -f wh1-values.yaml
```

3. Restart the StarRocks operator to make it aware of the new CRD:

```console
kubectl rollout restart deployment kube-starrocks-operator
```

## Manage the Warehouse

### Show the deployed Warehouse

If you have deployed the above warehouse, you can see it by using the following SQL command:

```console
# A warehouse has been created with the name `wh1`.
mysql> show warehouses;
+-------+-------------------+-----------+-----------+---------------------+-----------------+-----------------+------------+-----------+-----------+---------------------+---------------------+----------------------------------------------+
| Id    | Name              | State     | NodeCount | CurrentClusterCount | MaxClusterCount | StartedClusters | RunningSql | QueuedSql | CreatedOn | ResumedOn           | UpdatedOn           | Comment                                      |
+-------+-------------------+-----------+-----------+---------------------+-----------------+-----------------+------------+-----------+-----------+---------------------+---------------------+----------------------------------------------+
| 0     | default_warehouse | AVAILABLE | 0         | 1                   | 1               | 1               | 0          | 0         | NULL      | 2024-05-11 16:49:37 | 2024-05-11 17:53:30 | An internal warehouse init after FE is ready |
| 35030 | wh1               | AVAILABLE | 1         | 1                   | 1               | 1               | 0          | 0         | NULL      | NULL                | NULL                | NULL                                         |
+-------+-------------------+-----------+-----------+---------------------+-----------------+-----------------+------------+-----------+-----------+---------------------+---------------------+----------------------------------------------+
2 rows in set (0.00 sec)
```

### 3.2 Upgrade Deployment

We strongly recommend you to upgrade deployment by modifying the YAML Manifest file or values.yaml file. For example,
you can update any fields in the file, e.g. the image version, replicas, and resources.

> We don't suggest you to modify the deployment of warehouse by `kubectl edit`.

#### 3.2.1 Update the YAML manifest

For example, upgrade the image version:

```yaml
apiVersion: starrocks.com/v1
kind: StarRocksWarehouse
metadata:
  # A warehouse will be created with this name in StarRocks Cluster. If you are using dash(-) in the name, the warehouse
  # name created by StarRocks will be replaced with underscore(_).
  name: wh1

spec:
  # Make sure the StarRocks cluster exists in the same namespace.
  # You can check it by running `kubectl -n starrocks get starrocksclusters.starrocks.com`.
  starRocksCluster: kube-starrocks
  template:
    envVars:
      - name: TZ
        value: Asia/Shanghai
    image: us-west1-docker.pkg.dev/phrasal-verve-350013/celerdata/cn-ubuntu:3.2.7-ee  # this line is updated
    replicas: 1
    limits:
      cpu: 8
      memory: 8Gi
    requests:
      cpu: 8
      memory: 8Gi
```

Apply the updated YAML manifest:

```console
kubectl -n starrocks apply -f wh1.yaml
```

### 3.2.2 Update values.yaml for Helm chart

For example, upgrade the image version:

```yaml
# wh1-values.yaml
spec:
  # Make sure the StarRocks cluster exists in the same namespace.
  # You can check it by running `kubectl -n starrocks get starrocksclusters.starrocks.com`.
  starRocksClusterName: kube-starrocks
  replicas: 1
  image:
    repository: us-west1-docker.pkg.dev/phrasal-verve-350013/celerdata/cn-ubuntu
    tag: "3.2.7-ee" # this line is updated
  resources:
    limits:
      cpu: 8
      memory: 8Gi
    requests:
      cpu: 8
      memory: 8Gi
```

Then upgrade the warehouse by the following command:

```console
helm -n starrocks upgrade wh1 starrocks-community/warehouse -f wh1-values.yaml
```

## 4. Delete the Warehouse

If you deployed the warehouse by YAML manifest, you can delete it by running the following command:

```console
kubectl delete -f wh1.yaml
```

If you deployed the warehouse by Helm chart, you can delete it by running the following command:

```console
helm -n starrocks uninstall wh1
```