# Deploy StarRocks on Kubernetes with MiniKube and kubectl

This guide demonstrates how to deploy a StarRocks frontend-backend pair on Kubernetes with MiniKube with FQDN(See also: [Enable FQDN access](./administration/enable_fqdn.md)).

You can start StarRocks services in minutes with this deployment on a Minikube cluster.

The resources are deployed on Kubernetes with the following topology:

```mermaid
stateDiagram
  state MiniKubeCluster {
      fe_deployment --> fe_service
      be_deployment --> be_service
      be_deployment --> fe_deployment
      fe_deployment --> be_deployment
    }
  fe_service --> ExternalApp
 ```

This deployment exposed `starrocks-fe` service, and with minor modifications, you can also access the backend directly using the external IP.

**Note: This deployment is intended for testing and development purposes only. It is not recommended for production use. Data will be lost when the deployment is deleted, as data is not persisted by mounting volumes.**

## Prerequisites

`minikube` and `kubectl` are required in the deployment.

- [MiniKube](https://minikube.sigs.k8s.io/docs/start/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

## Start the Minikube cluster

Start the Minikube cluster:

```shell
minikube start
```

Launch the MiniKube dashboard to view the cluster status:

```shell
minikube dashboard
```

Start the MiniKube tunnel to access the services in the cluster using the external IP:

```shell
minikube tunnel
```

In this deployment, the `LoadBalancer` type service is used. It creates a load balancer in the cluster and exposes the service to the external network.

## Deploy StarrRocks

Download YAML file from [https://github.com/StarRocks/demo/blob/master/deploy/k8s/minikube-starrocks-fe-be-pair-fqdn.yaml](https://github.com/StarRocks/demo/blob/master/deploy/k8s/minikube-starrocks-fe-be-pair-fqdn.yaml). It is the YAML file used by `kubectl` to complete the deployment. Refer to the comments in the file for more information.

You can customize the YAML file to suit your needs if auto-scaling is not a concern. Alternatively, refer to [Deploy and Manage StarRocks on Kubernetes with StarRocks Operator](./sr_operator.md) for another solution.

Create resources for this deployment:

```shell
kubectl apply -f minikube-starrocks-fe-be-pair-fqdn.yaml
```

Wait for the deployment to complete. You can check the deployment status with the following command:

```shell
kubectl get deployment.apps --selector=demo=starrocks-pair
```

## Access StarRocks

Obtain the external IPs of the services, which can be used to access them:

```shell
kubectl get service --selector=demo=starrocks-pair
```

Access the access the frontend by `http://<starrocks-fe-service-external-ip>:8030`:

```shell
mysql --connect-timeout 2 -h <starrocks-fe-service-external-ip> -P9030 -uroot
```

## Explanation

### FQDN

In this deployment, FQDNs are used for interactions between `starrrocks-fe-deployment` and `starrrocks-be-deployment`. The current version of StarRocks does not support short hostnames in Kubernetes.

The `subdomain` fields in `starrrocks-fe-deployment` and `starrocks-be-deployment` are required to set valid FQDNs for the frontend and backend. **The FQDN of the backend deployment should be specified** instead of other host names when adding the backend to the frontend. The FQDN of the frontend deployment is automatically handled by the start script of the frontend image.

### Health check

An `initContainer` was used for the frontend health check when deploying the backend. The health check is performed using the `curl` command. The `curl` command will fail if the frontend is not ready. The `initContainer` retries the health check until the frontend is ready. The frontend also performs health checks with `curl` by specifying `livenessProbe` in the YAML file. See also: [Manage a cluster](./administration/Cluster_administration.md)

Other specifications are not unique to StarRocks deployment. See also: [Kubernetes](https://kubernetes.io/docs/concepts/overview/what-is-kubernetes/)

## YAML examples

If you prefer not to download the YAML file and would rather view some examples on this page, here are a few extracted examples.

Deploy backend behind the service:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    starrocks: be
    demo: starrocks-pair
  name: starrocks-be
spec:
  replicas: 1
  selector:
    matchLabels:
      starrocks: be
  template:
    metadata:
      labels:
        starrocks: be
    spec:
      # create `be` container when `fe` service is healthy
      initContainers:
        - name: wait-for-fe
          image: curlimages/curl:8.00.1
          args:
            - /bin/sh
            - -c
            - |
              while true; do
                response=$(curl -s http://$FE_HOST:8030/api/bootstrap)
                status=$(echo "$response" | grep -Eo '"status":"[^"]+"' | cut -d '"' -f 4)
                if [ "$status" = "OK" ]; then
                  break
                else
                  sleep 10
                fi
              done
          envFrom:
            - configMapRef:
                name: starrocks-deploy-envs
      # when the `fe` service is healthy, it's safe to add the `be` FQDN:PORT as a backend of `fe` and then start it.
      containers:
        - args:
            - /bin/bash
            - -c
            - |
              mysql --connect-timeout 2 -h $FE_HOST -P9030 -uroot -e "alter system add backend \"$BE_HOST-deployment.starrocks-be.default.svc.cluster.local:9050\";"
              /opt/starrocks/be/bin/start_be.sh
          image: starrocks/be-ubuntu:2.5.4
          name: starrocks-be-container
          ports:
            - containerPort: 8040
            - containerPort: 9050
            - containerPort: 9060
          resources: {}
          envFrom:
            - configMapRef:
                name: starrocks-deploy-envs
      hostname: starrocks-be-deployment
      # subdomain is required to let starrocks access `be` deployment by FQDN
      subdomain: starrocks-be
      restartPolicy: Always
```

Deploy frontend behind the service:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    starrocks: fe
    demo: starrocks-pair
  name: starrocks-fe
spec:
  replicas: 1
  selector:
    matchLabels:
      starrocks: fe
  template:
    metadata:
      labels:
        starrocks: fe
    spec:
      # start `fe` as FQDN host_type
      containers:
        - args:
            - /bin/sh
            - -c
            - |
              /opt/starrocks/fe/bin/start_fe.sh --host_type FQDN
          image: starrocks/fe-ubuntu:2.5.4
          livenessProbe:
            exec:
              command:
                - /bin/sh
                - -c
                - 'response=$(curl -s http://$FE_HOST:8030/api/bootstrap) && status=$(echo "$response" | grep -Po ''"status":.*?[^\\]",'' | cut -d ''"'' -f 4) && [ "$status" = "OK" ]'
            initialDelaySeconds: 5
            failureThreshold: 30
            periodSeconds: 5
            timeoutSeconds: 5
          name: starrocks-fe-container
          ports:
            - containerPort: 8030
            - containerPort: 9010
            - containerPort: 9020
            - containerPort: 9030
          resources: {}
          volumeMounts:
            - mountPath: /opt/starrocks/fe/conf/fe.conf
              subPath: fe.conf
              name: fe-conf-volume
          envFrom:
            - configMapRef:
                name: starrocks-deploy-envs
      hostname: starrocks-fe-deployment
      # subdomain is required to let starrocks `be` access `fe` deployment by FQDN
      subdomain: starrocks-fe
      volumes:
        - name: fe-conf-volume
          configMap:
            name: fe-config
      restartPolicy: Always
```

Deploy frontend service:

```yaml
apiVersion: v1
kind: Service
metadata:
  labels:
    starrocks: fe
    demo: starrocks-pair
  name: starrocks-fe
spec:
  # exposed ports
  type: LoadBalancer
  ports:
    - name: "http"
      port: 8030
      targetPort: 8030
    - name: "rpc"
      port: 9020
      targetPort: 9020
    - name: "query"
      port: 9030
      targetPort: 9030
  selector:
    starrocks: fe
```
