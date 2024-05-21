# Automatic scaling for CN nodes

This topic describes how to configure automatic scaling for CN nodes in a StarRocks cluster.

## Prerequisites

Configuring automatic scaling for CN nodes requires:

- A new or existing deployment of the operator using the Helm chart.
- The [Kubernetes metrics server](https://github.com/kubernetes-sigs/metrics-server) is installed.

:::tip
The Kubernetes metrics server is not installed by default. You can check to see if it is installed with:

```
kubectl get service metrics-server -n kube-system
```

You can install it by following the instructions in the [Kubernetes metrics server documentation](https://github.com/kubernetes-sigs/metrics-server).
:::

## Configure automatic scaling for CN nodes in `values.yaml`

Add the following snippets to `values.yaml` to configure the automatic scaling policy for CN nodes,

```YAML
  starrocksCluster: # do not forget to set enabledCn to true to enable deployment of CNs.
    enabledCn: true

  starrocksCnSpec:
    image:
      repository: us-west1-docker.pkg.dev/phrasal-verve-350013/celerdata/cn-ubuntu
      tag: "3.2.6-ee"
    resources:
      requests:
        cpu: 4
        memory: 4Gi
    autoScalingPolicy:
      minReplicas: 1
      maxReplicas: 10
      hpaPolicy:
        metrics: # Resource metrics
          - type: Resource
            resource:
              name: memory # The average memory usage of CNs is specified as a resource metric.
              target:
                averageUtilization: 30
                # The elastic scaling threshold is 30%.
                # When the average memory utilization of CNs exceeds 30%, the number of CNs increases for scale-out.
                # When the average memory utilization of CNs is below 30%, the number of CNs decreases for scale-in.
                type: Utilization
          - type: Resource
            resource:
              name: cpu # The average CPU utilization of CNs is specified as a resource metric.
              target:
                averageUtilization: 60
                # The elastic scaling threshold is 60%.
                # When the average CPU utilization of CNs exceeds 60%, the number of CNs increases for scale-out.
                # When the average CPU utilization of CNs is below 60%, the number of CNs decreases for scale-in.
                type: Utilization
        behavior: #  The scaling behavior is customized according to business scenarios, helping you achieve rapid or slow scaling or disable scaling.
          scaleUp:
            policies:
              - type: Pods
                value: 1
                periodSeconds: 10
          scaleDown:
            selectPolicy: Disabled
```

## Fields description

The following are descriptions of a few important fields:

- The upper and lower limits for elastic scaling.

  ```YAML
  maxReplicas: 10 # The maximum number of CNs is set to 10.
  minReplicas: 1 # The minimum number of CNs is set to 1.
  ```

- The threshold for elastic scaling.

  ```YAML
  # For example, the average CPU utilization of CNs is specified as a resource metric.
  # The elastic scaling threshold is 60%.
  # When the average CPU utilization of CNs exceeds 60%, the number of CNs increases for scale-out.
  # When the average CPU utilization of CNs is below 60%, the number of CNs decreases for scale-in.
  - type: Resource
    resource:
      name: cpu
      target:
        averageUtilization: 60
        type: Utilization
  ```

- The `behavior` for elastic scaling.

  Kubernetes also supports using `behavior` to customize scaling behaviors according to business scenarios, helping you
  achieve rapid or slow scaling or disable scaling. For more information about automatic scaling policies,
  see [Horizontal Pod Scaling](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/).
