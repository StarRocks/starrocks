---
displayed_sidebar: "English"
description: Use Helm to deploy StarRocks
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import OperatorPrereqs from '../assets/deployment/_OperatorPrereqs.mdx'
import DDL from '../assets/quick-start/_DDL.mdx'
import Clients from '../assets/quick-start/_clientsAllin1.mdx'
import SQL from '../assets/quick-start/_SQL.mdx'
import Curl from '../assets/quick-start/_curl.mdx'

# StarRocks with Helm

This tutorial covers:

- Deploying the StarRocks Kubernetes Operator and a StarRocks deployment with Helm
- Loading two public datasets including basic transformation of the data
- Analyzing the data with SELECT and JOIN
- Basic data transformation (the **T** in ETL)

:::tip
The datasets and queries are the same as the ones used in the Basic Quick Start. The main difference here is deploying with Helm and the StarRocks Operator.
:::

The data used is provided by NYC OpenData and the National Centers for Environmental Information.

Both of these datasets are large, and because this tutorial is intended to help you get exposed to working with StarRocks we are not going to load data for the past 120 years. You can run this with a GKE Kubernetes cluster built on three e2-standard-4 machines (or similar) with 80GB disk. For larger fault-tolerant and scalable deployments we have other documentation and will provide that later.

There is a lot of information in this document, and it is presented with step-by-step content at the beginning, and the technical details at the end. This is done to serve these purposes in this order:

1. Allow the reader to load data in StarRocks and analyze that data.
2. Explain the basics of data transformation during loading.

---

## Prerequisites

<OperatorPrereqs />

### SQL client

You can use the SQL client provided in the Kubernetes environment, or use one on your system. Many MySQL-compatible clients will work, and this guide covers the configuration of DBeaver and MySQL WorkBench.

### curl

`curl` is used to issue the data load job to StarRocks, and to download the datasets. Check to see if you have it installed by running `curl` or `curl.exe` at your OS prompt. If curl is not installed, [get curl here](https://curl.se/dlwiz/?type=bin).

---

## Terminology

### FE

Frontend nodes are responsible for metadata management, client connection management, query planning, and query scheduling. Each FE stores and maintains a complete copy of metadata in its memory, which guarantees indiscriminate services among the FEs.

### BE

Backend nodes are responsible for both data storage and executing query plans.

---

## Deploy StarRocks with Helm

## Procedure

1. Add the Helm Chart Repo for StarRocks. The Helm Chart contains the definitions of the StarRocks Operator and the custom resource StarRocksCluster.
   1. Add the Helm Chart Repo.

      ```Bash
      helm repo add starrocks-community https://starrocks.github.io/starrocks-kubernetes-operator
      ```

   2. Update the Helm Chart Repo to the latest version.

      ```Bash
      helm repo update
      ```

   3. View the Helm Chart Repo that you added.

      ```Bash
      helm search repo starrocks-community
      ```

      ```
      NAME                              	CHART VERSION	APP VERSION	DESCRIPTION
      starrocks-community/kube-starrocks	1.9.7        	3.2-latest 	kube-starrocks includes two subcharts, operator...
      starrocks-community/operator      	1.9.7        	1.9.7      	A Helm chart for StarRocks operator
      starrocks-community/starrocks     	1.9.7        	3.2-latest 	A Helm chart for StarRocks cluster
      starrocks-community/warehouse     	1.9.7        	3.2-latest 	Warehouse is currently a feature of the StarRoc...
      ```

---

## Download the data

Download these two datasets to your machine.

### New York City crash data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

### Weather data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---






```bash
kubectl get pods
```

```
NAME                                     READY STATUS  RESTARTS AGE
kube-starrocks-operator-58bdf9bb55-d4qd8 1/1   Running 0        3m3s
quickstart-be-0                          0/1   Running 0        118s
quickstart-be-1                          0/1   Running 0        117s
quickstart-be-2                          1/1   Running 0        117s
quickstart-fe-0                          1/1   Running 0        2m43s
quickstart-fe-1                          1/1   Running 0        2m43s
quickstart-fe-2                          1/1   Running 0        2m43s
quickstart-fe-proxy-787fd777cf-r2rr7     1/1   Running 0        117s
```

```bash
kubectl get starrocksclusters.starrocks.com
```

```bash
NAME       PHASE    FESTATUS  BESTATUS  CNSTATUS  FEPROXYSTATUS
quickstart running  running   running             running
```

```bash
kubectl get services
```

```bash
NAME                        TYPE         CLUSTER-IP     EXTERNAL-IP   PORT(S)
kubernetes                  ClusterIP    34.118.224.1   <none>        443/TCP
quickstart-be-search        ClusterIP    None           <none>        9050/TCP
quickstart-be-service       ClusterIP    34.118.230.192 <none>        9060/TCP,8040/TCP,9050/TCP,8060/TCP
quickstart-fe-proxy-service LoadBalancer 34.118.232.134 34.176.52.103 8080:30546/TCP
quickstart-fe-search        ClusterIP    None           <none>        9030/TCP
quickstart-fe-service       ClusterIP    34.118.238.5   <none>        8030/TCP,9020/TCP,9030/TCP,9010/TCP
```

TEST RUN

## Add the `starrocks-community` Helm repo

```bash
helm repo add starrocks-community https://starrocks.github.io/starrocks-kubernetes-operator
helm repo update starrocks-community
helm search repo starrocks-community
```

```
NAME                              	CHART VERSION	APP VERSION	DESCRIPTION
starrocks-community/kube-starrocks	1.9.7        	3.2-latest 	kube-starrocks includes two subcharts, operator...
starrocks-community/operator      	1.9.7        	1.9.7      	A Helm chart for StarRocks operator
starrocks-community/starrocks     	1.9.7        	3.2-latest 	A Helm chart for StarRocks cluster
starrocks-community/warehouse     	1.9.7        	3.2-latest 	Warehouse is currently a feature of the StarRoc...
```
## Download the Helm values files

There is a default Helm `values.yaml` file and a custom values file that overrides some of the defaults. For now look at the custom file and the defaults will be provided at the end of this quick start lab.

The default `values.yaml` file deploys a single FE and a single BE. For this lab you will:

- Configure a password for the StarRocks database user `root`
- Provide for high-availability with three FEs and three BEs
- Configure a LoadBalancer to allow MySQL clients to connect from outside the Kubernetes cluster
- Configure a LoadBalancer to allow loading data from outside the Kubernetes cluster

### Password for the database user

```yaml
starrocks:
    initPassword:
        enabled: true
            # Set a password secret, for example:
            # kubectl create secret generic starrocks-root-pass --from-literal=password='g()()dpa$$word'
        passwordSecret: starrocks-root-pass
```

### High Availability with 3 FEs and 3 BEs

```yaml
starrocks:
    starrocksFESpec:
        replicas: 3
        service:
            type: LoadBalancer
        resources:
            requests:
                cpu: 1
                memory: 1Gi

    starrocksBeSpec:
        replicas: 3
        resources:
            requests:
                cpu: 1
                memory: 2Gi
        storageSpec:
            storageSize: 15Gi
```

### LoadBalancer for MySQL clients

```yaml
starrocks:
    starrocksFESpec:
        service:
            type: LoadBalancer
```

### LoadBalancer for external data loading

```yaml
starrocks:
    starrocksFeProxySpec:
        enabled: true
        service:
            type: LoadBalancer
```

### The complete values file

Putting together the above snippets:

```yaml
starrocks:
    initPassword:
        enabled: true
            # Set a password secret, for example:
            # kubectl create secret generic starrocks-root-pass --from-literal=password='g()()dpa$$word'
        passwordSecret: starrocks-root-pass

    starrocksFESpec:
        replicas: 3
        service:
            type: LoadBalancer
        resources:
            requests:
                cpu: 1
                memory: 1Gi

    starrocksBeSpec:
        replicas: 3
        resources:
            requests:
                cpu: 1
                memory: 2Gi
        storageSpec:
            storageSize: 15Gi

    starrocksFeProxySpec:
        enabled: true
        service:
            type: LoadBalancer
```

## Set the StarRocks root database user password

To load data from outside of the Kubernetes cluster the StarRocks database will be exposed externally. You should set
a password for the StarRocks database user `root`. The operator will apply the password to the FE and BE nodes.

```bash
kubectl create secret generic starrocks-root-pass --from-literal=password='g()()dpa$$word'
```

```
secret/starrocks-root-pass created
```

```bash
helm install -f ha_cluster_with_proxy_and_password.yaml starrocks starrocks-community/kube-starrocks
```

```
NAME: starrocks
LAST DEPLOYED: Wed Jun 26 20:25:09 2024
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
Thank you for installing kube-starrocks-1.9.7 kube-starrocks chart.
It will install both operator and starrocks cluster, please wait for a few minutes for the cluster to be ready.

Please see the values.yaml for more operation information: https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/helm-charts/charts/kube-starrocks/values.yaml
```

## Check the status of the StarRocks cluster

You can check the progress with these commands:

```bash
kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
```

```
NAME             PHASE         FESTATUS      BESTATUS      CNSTATUS   FEPROXYSTATUS
kube-starrocks   reconciling   reconciling   reconciling              reconciling
```

```bash
kubectl get pods
```

:::note
The `kube-starrocks-initpwd` pod will go through `error` and `CrashLoopBackOff` states as it attempts to connect to the FE and BE pods to set the StarRocks root password. You should ignore these errors and wait for a status of `Completed` for this pod.
:::

```
NAME                                       READY   STATUS             RESTARTS      AGE
kube-starrocks-be-0                        0/1     Running            0             20s
kube-starrocks-be-1                        0/1     Running            0             20s
kube-starrocks-be-2                        0/1     Running            0             20s
kube-starrocks-fe-0                        1/1     Running            0             66s
kube-starrocks-fe-1                        0/1     Running            0             65s
kube-starrocks-fe-2                        0/1     Running            0             66s
kube-starrocks-fe-proxy-56f8998799-d4qmt   1/1     Running            0             20s
kube-starrocks-initpwd-m84br               0/1     CrashLoopBackOff   3 (50s ago)   92s
kube-starrocks-operator-54ffcf8c5c-xsjc8   1/1     Running            0             92s
```

## Verify that the cluster is healthy

```bash
kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
```

```
NAME             PHASE     FESTATUS   BESTATUS   CNSTATUS   FEPROXYSTATUS
kube-starrocks   running   running    running               running
```

```bash
kubectl get pods
```

:::tip
The system is ready when all of the pods except for `kube-starrocks-initpwd` show `1/1` in the `READY` column. The `kube-starrocks-initpwd` pod should show `0/1` and a `STATUS` of `Completed`.
:::

```
NAME                                       READY   STATUS      RESTARTS   AGE
kube-starrocks-be-0                        1/1     Running     0          57s
kube-starrocks-be-1                        1/1     Running     0          57s
kube-starrocks-be-2                        1/1     Running     0          57s
kube-starrocks-fe-0                        1/1     Running     0          103s
kube-starrocks-fe-1                        1/1     Running     0          102s
kube-starrocks-fe-2                        1/1     Running     0          103s
kube-starrocks-fe-proxy-56f8998799-d4qmt   1/1     Running     0          57s
kube-starrocks-initpwd-m84br               0/1     Completed   4          2m9s
kube-starrocks-operator-54ffcf8c5c-xsjc8   1/1     Running     0          2m9s
```



```
kubectl get services
NAME                              TYPE           CLUSTER-IP       EXTERNAL-IP     PORT(S)                                                       AGE
kube-starrocks-be-search          ClusterIP      None             <none>          9050/TCP                                                      78s
kube-starrocks-be-service         ClusterIP      34.118.228.231   <none>          9060/TCP,8040/TCP,9050/TCP,8060/TCP                           78s
kube-starrocks-fe-proxy-service   LoadBalancer   34.118.230.176   34.176.12.205   8080:30241/TCP                                                78s
kube-starrocks-fe-search          ClusterIP      None             <none>          9030/TCP                                                      2m4s
kube-starrocks-fe-service         LoadBalancer   34.118.226.82    34.176.215.97   8030:30620/TCP,9020:32461/TCP,9030:32749/TCP,9010:30911/TCP   2m4s
kubernetes                        ClusterIP      34.118.224.1     <none>          443/TCP                                                       8h
```

---

### Connect to StarRocks with a SQL client

NOTE: Add info about `kubectl get services` and the external IP.

:::tip

If you are using a client other than the mysql CLI, open that now.
:::

This command will run the `mysql` command in a Kubernetes pod:

```sql
kubectl exec --stdin --tty kube-starrocks-fe-0 -- \
  mysql -P9030 -h127.0.0.1 -u root --prompt="StarRocks > "
```

---
## Create some tables

```bash
mysql -h 34.176.215.97 -P9030 -uroot -p
```


<DDL />

Exit from the MySQL client, or open a new shell to run commands at the command line to upload data.

```sql
exit
```



## Upload data

There are many ways to load data into StarRocks. For this tutorial, the simplest way is to use curl and StarRocks Stream Load.


Export the external IP and port number of the FE Proxy service in an environment variable `FE_PROXY` so that you can use the proxy in the `curl` commands to load the data with Stream Load.

```bash
export FE_PROXY=34.176.12.205:8080
```

Upload the two datasets that you downloaded earlier.

There are many ways to load data into StarRocks. For this tutorial the simplest way is to use curl and StarRocks Stream Load.

:::tip
Open a new shell as these curl commands are run at the operating system prompt, not in the `mysql` client. The commands refer to the datasets that you downloaded, so run them from the directory where you downloaded the files.

You will be prompted for a password. Use the password that you added to the Kubernetes secret `starrocks-root-pass`. If you used the command provided, the password is `g()()dpa$$word`.
:::

The `curl` commands look complex, but they are explained in detail at the end of the tutorial. For now, we recommend running the commands and running some SQL to analyze the data, and then reading about the data loading details at the end.


```bash
curl --location-trusted -u root             \
    -T ./NYPD_Crash_Data.csv                \
    -H "label:crashdata-0"                  \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns:tmp_CRASH_DATE, tmp_CRASH_TIME, CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5" \
    # highlight-next-line
    -XPUT http://$FE_PROXY/api/quickstart/crashdata/_stream_load
```

```
Enter host password for user 'root':
{
    "TxnId": 2,
    "Label": "crashdata-0",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 423726,
    "NumberLoadedRows": 423725,
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 96227746,
    "LoadTimeMs": 2483,
    "BeginTxnTimeMs": 42,
    "StreamLoadPlanTimeMs": 122,
    "ReadDataTimeMs": 1610,
    "WriteDataTimeMs": 2253,
    "CommitAndPublishTimeMs": 65,
    "ErrorURL": "http://kube-starrocks-be-2.kube-starrocks-be-search.default.svc.cluster.local:8040/api/_load_error_log?file=error_log_5149e6f80de42bcb_eab2ea77276de4ba"
}
```

```bash
curl --location-trusted -u root             \
    -T ./72505394728.csv                    \
    -H "label:weather-0"                    \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, REPORT_TYPE, SOURCE, HourlyAltimeterSetting, HourlyDewPointTemperature, HourlyDryBulbTemperature, HourlyPrecipitation, HourlyPresentWeatherType, HourlyPressureChange, HourlyPressureTendency, HourlyRelativeHumidity, HourlySkyConditions, HourlySeaLevelPressure, HourlyStationPressure, HourlyVisibility, HourlyWetBulbTemperature, HourlyWindDirection, HourlyWindGustSpeed, HourlyWindSpeed, Sunrise, Sunset, DailyAverageDewPointTemperature, DailyAverageDryBulbTemperature, DailyAverageRelativeHumidity, DailyAverageSeaLevelPressure, DailyAverageStationPressure, DailyAverageWetBulbTemperature, DailyAverageWindSpeed, DailyCoolingDegreeDays, DailyDepartureFromNormalAverageTemperature, DailyHeatingDegreeDays, DailyMaximumDryBulbTemperature, DailyMinimumDryBulbTemperature, DailyPeakWindDirection, DailyPeakWindSpeed, DailyPrecipitation, DailySnowDepth, DailySnowfall, DailySustainedWindDirection, DailySustainedWindSpeed, DailyWeather, MonthlyAverageRH, MonthlyDaysWithGT001Precip, MonthlyDaysWithGT010Precip, MonthlyDaysWithGT32Temp, MonthlyDaysWithGT90Temp, MonthlyDaysWithLT0Temp, MonthlyDaysWithLT32Temp, MonthlyDepartureFromNormalAverageTemperature, MonthlyDepartureFromNormalCoolingDegreeDays, MonthlyDepartureFromNormalHeatingDegreeDays, MonthlyDepartureFromNormalMaximumTemperature, MonthlyDepartureFromNormalMinimumTemperature, MonthlyDepartureFromNormalPrecipitation, MonthlyDewpointTemperature, MonthlyGreatestPrecip, MonthlyGreatestPrecipDate, MonthlyGreatestSnowDepth, MonthlyGreatestSnowDepthDate, MonthlyGreatestSnowfall, MonthlyGreatestSnowfallDate, MonthlyMaxSeaLevelPressureValue, MonthlyMaxSeaLevelPressureValueDate, MonthlyMaxSeaLevelPressureValueTime, MonthlyMaximumTemperature, MonthlyMeanTemperature, MonthlyMinSeaLevelPressureValue, MonthlyMinSeaLevelPressureValueDate, MonthlyMinSeaLevelPressureValueTime, MonthlyMinimumTemperature, MonthlySeaLevelPressure, MonthlyStationPressure, MonthlyTotalLiquidPrecipitation, MonthlyTotalSnowfall, MonthlyWetBulb, AWND, CDSD, CLDD, DSNW, HDSD, HTDD, NormalsCoolingDegreeDay, NormalsHeatingDegreeDay, ShortDurationEndDate005, ShortDurationEndDate010, ShortDurationEndDate015, ShortDurationEndDate020, ShortDurationEndDate030, ShortDurationEndDate045, ShortDurationEndDate060, ShortDurationEndDate080, ShortDurationEndDate100, ShortDurationEndDate120, ShortDurationEndDate150, ShortDurationEndDate180, ShortDurationPrecipitationValue005, ShortDurationPrecipitationValue010, ShortDurationPrecipitationValue015, ShortDurationPrecipitationValue020, ShortDurationPrecipitationValue030, ShortDurationPrecipitationValue045, ShortDurationPrecipitationValue060, ShortDurationPrecipitationValue080, ShortDurationPrecipitationValue100, ShortDurationPrecipitationValue120, ShortDurationPrecipitationValue150, ShortDurationPrecipitationValue180, REM, BackupDirection, BackupDistance, BackupDistanceUnit, BackupElements, BackupElevation, BackupEquipment, BackupLatitude, BackupLongitude, BackupName, WindEquipmentChangeDate" \
    # highlight-next-line
    -XPUT http://$FE_PROXY/api/quickstart/weatherdata/_stream_load
```

```
Enter host password for user 'root':
{
    "TxnId": 4,
    "Label": "weather-0",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 22931,
    "NumberLoadedRows": 22931,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 15558550,
    "LoadTimeMs": 404,
    "BeginTxnTimeMs": 1,
    "StreamLoadPlanTimeMs": 7,
    "ReadDataTimeMs": 157,
    "WriteDataTimeMs": 372,
    "CommitAndPublishTimeMs": 23
}
```

## Connect with a MySQL client

Connect with a MySQL client if you are not connected. Remember to use the external IP address of the `kube-starrocks-fe-service` service and the password that you configured in the Kubernetes secret `starrocks-root-pass`.

```bash
mysql -h 34.176.215.97 -P9030 -uroot -p
```

## Answer some questions

<SQL />

```sql
exit
```

## Cleanup

Run this command if you are finished and would like to remove the StarRocks cluster and the StarRocks operator.

```bash
helm delete starrocks
```


---

## Summary

In this tutorial you:

- Deployed StarRocks with Helm and the StarRocks Operator
- Loaded crash data provided by New York City and weather data provided by NOAA
- Analyzed the data using SQL JOINs to find out that driving in low visibility or icy streets is a bad idea

There is more to learn; we intentionally glossed over the data transformation done during the Stream Load. The details on that are in the notes on the curl commands below.

---

## Notes on the curl commands

<Curl />

---

## More information

default `values.yaml`

[StarRocks table design](../table_design/StarRocks_table_design.md)

[Materialized views](../cover_pages/mv_use_cases.mdx)

[Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)

The [Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) dataset is provided by New York City subject to these [terms of use](https://www.nyc.gov/home/terms-of-use.page) and [privacy policy](https://www.nyc.gov/home/privacy-policy.page).

The [Local Climatological Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD) is provided by NOAA with this [disclaimer](https://www.noaa.gov/disclaimer) and this [privacy policy](https://www.noaa.gov/protecting-your-privacy).

[Helm](https://helm.sh/) is a package manager for Kubernetes. A [Helm Chart](https://helm.sh/docs/topics/charts/) is a Helm package and contains all of the resource definitions necessary to run an application on a Kubernetes cluster. This topic describes how to use Helm to automatically deploy a StarRocks cluster on a Kubernetes cluster.


## Next steps

- Access StarRocks cluster

  You can access the StarRocks cluster from inside and outside the Kubernetes cluster. For detailed instructions, see [Access StarRocks Cluster./sr_operator.md#access-starrocks-cluster).

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

    :::info

    If persistent volumes are not mounted, the StarRocks Operator will use emptyDir to store FE metadata and logs, as well as BE data and logs. When containers restart, data will be lost.

    :::

  - If you need to set the root user password:

    - Manually set the root user's password after deploying the StarRocks cluster, see [Change root user password HOWTO](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/change_root_password_howto.md).

    - Automatically set the root user's password when deploying the StarRocks cluster, see [Initialize root user password](https://github.com/StarRocks/starrocks-kubernetes-operator/blob/main/doc/initialize_root_password_howto.md).


- The address of Helm Chart maintained by StarRocks on Artifact Hub: [kube-starrocks](https://artifacthub.io/packages/helm/kube-starrocks/kube-starrocks).