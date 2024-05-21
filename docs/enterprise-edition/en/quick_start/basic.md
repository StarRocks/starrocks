import DDL from '../assets/quick-start/_DDL.mdx'
import Clients from '../assets/quick-start/_clientsAllin1.mdx'
import SQL from '../assets/quick-start/_SQL.mdx'
import Curl from '../assets/quick-start/_curl.mdx'

# Basic Quick Start

## Overview

This is a Quick Start lab to introduce you to:

- using Helm to deploy the CelerData Enterprise Edition Kubernetes operator
- Configuring an initial database password
- Enabling SSL support for user authentication
- Configuring storage
- Configuring a proxy service for stream load
- Deploying a StarRocks cluster
- Loading some public datasets
- Analyzing the data with SQL JOINs

## Prerequisites

### Kubernetes environment

The Kubernetes environment used while writing this guide consists of three nodes with four vCPUS, and 16GB RAM each (GCP `e2-standard-4` machines). The Kubernetes cluster was deployed with this `gcloud` command:

```bash
gcloud container --project enterprise-demo-422514 \
  clusters create ee-docs \
  --location=southamerica-west1-b \
  --machine-type e2-standard-4 --disk-size 80 --num-nodes 3
```

### Helm

Helm is a package manager for Kubernetes that simplifies the deployment and management of applications. In this lab you will use Helm to deploy the CelerData Enterprise Edition Kubernetes operator and the sample StarRocks cluster.

[Install helm](https://helm.sh/docs/intro/quickstart/)

### Sample `values.yaml` for Helm

A sample `values.yaml` file is in the [documentation samples](https://github.com/StarRocks/demo/tree/master/documentation-samples/enterprise-edition/operator) and contains the edits shown in this guide. Please download the file.

### CelerData registry configuration

A CelerData registry key is required to deploy the Enterprise Edition operator. The key allows the operator to pull the Enterprise Edition images from the CelerData registry. If you do not have a key for the Enterprise Edition image, open a [case in Zendesk](https://support.celerdata.com/hc/en-us/requests/new) to request your registry key.

## Notes about this lab

In this lab you will modify the default Helm `values.yaml` file to:

- Configure an initial password for the StarRocks database `root` account.
- Enable SSL support for user authentication.
- Configure storage using persistent volume claims.

Then you will use Helm to:

- Deploy the CelerData Enterprise Edition Kubernetes operator.
- Deploy a StarRocks cluster.

Once the StarRocks Enterprise Edition

- Load some public datasets.
- Analyze the data with SQL JOINs.

## Secrets

### Configure access to the Enterprise Edition image

The file `celerdata-registry-config.json` is the configuration file from CelerData Support that allows you to
use the Enterprise Edition software. If you do not have this file please open a Zendesk case and ask for your
file.

```
cat ./Downloads/celerdata-registry-config.json
```

```json
{
	"auths": {
		"us-west1-docker.pkg.dev": {
			"auth": "X2pz
            ...
           IKfQ=="
		}
	}
}%
```


```
kubectl create secret generic regcred \
  --from-file=.dockerconfigjson=./celerdata-registry-config.json \
  --type=kubernetes.io/dockerconfigjson
```

:::tip

- The secret name is `regcred`
- `--from-file` takes one argument consisting of two strings separated by an `=`:
  - The string before the `=` is `.dockerconfigjson` and this is the name of the key
  - The string after the `=` is `celerdata-registry-config.json`, and this is the filename of the secret provided by CelerData support
- The type of secret is `kubernetes.io/dockerconfigjson`

:::

```
secret/regcred created
```

### Set up the database `root` account password

The initial password for the `root` account can be specified in a Kubernetes secret, or directly in the `values.yaml`.

#### Use a Kubernetes secret for the initial root password

Edit the `initPassword` section of `values.yaml`:

```
  initPassword:
    enabled: true
    # The secret name that contains password, the key of the secret is "password", and you should create it first.
    passwordSecret: starrocks-root-password
```

Create the secret `starrocks-root-password` using the key `password`. You can use any secret creation method supported by your Kubernetes environment, in this example the secret is created from a file containing the password `g()()dpa$$word`:

```
echo -n 'g()()dpa$$word' > ./password.txt
kubectl create secret generic starrocks-root-password  --from-file=password=./password.txt
```

Alternatively, you can specify the password directly in values.yaml:

```yaml
  initPassword:
    enabled: true
    password: "password"
```

:::note
Comment out the `passwordSecret:` entry if you are adding the initial password directly in `values.yaml`.
:::

## Configure TLS

To configure TLS you need a certificate in `.jks` format. Follow the policies of your organization. For this tutorial you can
choose to use a self-signed certificate. This certificate is not appropriate for production work.

```
keytool -genkeypair \
        -alias starrocks \
        -keypass starrocks \
        -keyalg RSA \
        -keysize 1024 \
        -validity 365 \
        -keystore starrocks.jks \
        -storepass starrocks
```

Add the newly created file `starrocks.jks` as a Kubernetes secret:

```
kubectl create secret generic keystore --from-file=data=./starrocks.jks
```

```
secret/keystore created
```

## Update `values.yaml` for SSL

Add the SSL configuration and mount the secret `keystore` in your FE nodes.

Add these entries to the **`starrocks.starrocksFESpec.config`** section of `values.yaml`

```
      # SSL added configurations
      ssl_keystore_location=/etc/starrocks/keystore/data
      ssl_keystore_password=starrocks
      ssl_key_password=starrocks
```

Add these entries to the **`starrocks.starrocksFESpec.secrets`** section of `values.yaml`. Make sure to use the `keypass` and `ssl_key_password` used when you created your keystore.

```
    #secrets: []
    # Mount the secret for the keystore
    secrets:
      - name: keystore
        mountPath: /etc/starrocks/keystore
```

## Update `values.yaml` for storage

Enable the use of persistent storage by setting `starrocks.starrocksFESpec.storageSpec.name` and `starrocks.starrocksBeSpec.storageSpec.name` to non-empty strings. The string will be used as the prefix for the names of the volumes created for the FEs and BEs respectively.

For example, if you set `starrocks.starrocksBeSpec.storageSpec.name` to `be-storage`, then the data volume for pod `kube-starrocks-be-0` will be named `be-storage-data-kube-starrocks-be-0`. Similarly, the log storage volume will be named `be-storage-log-kube-starrocks-be-0`.

Set the size of the volumes. The `values.yaml` file specifies sizes that will work fine for this guide.
The settings to check are
`starrocks.starrocksFESpec.storageSpec.storageSize` and `starrocks.starrocksFESpec.storageSpec.logStorageSize` under `starrocks.starrocksFESpec` and `starrocks.starrocksFESpec.storageSpec.storageSize` and `starrocks.starrocksFESpec.storageSpec.logStorageSize` under `starrocks.starrocksBeSpec`.

:::tip
Set `storageSpec.storageClassName` if you do not want to use the default persistent storage class.
:::

#### FE storage

```yaml
    storageSpec:
      # Specifies the name prefix of the volumes to mount. If left unspecified,
      # `emptyDir` volumes will be used by default, which are ephemeral and data
      # will be lost on pod restart.
      #
      # For persistent storage, specify a volume name prefix.
      # For example, using `fe` as the name prefix would be appropriate.
      name: "fe-storage"
```

#### BE storage

```yaml
    storageSpec:
      # Specifies the name prefix of the volumes to mount. If left unspecified,
      # `emptyDir` volumes will be used by default, which are ephemeral and data
      # will be lost on pod restart.
      #
      # For persistent storage, specify a volume name prefix.
      # For example, using `be` as the name prefix would be appropriate.
      name: "be-storage"
```


## Update `values.yaml` to deploy a reverse proxy

If you are using Stream Load you will need a reverse proxy to load data from outside the Kubernetes cluster. To enable the proxy set **starrocksFeProxySpec** > **enabled** to `true`, and **starrocksFeProxySpec** > **service** > **type** to `LoadBalancer`.

```yaml
  # specify the fe proxy deploy or not.
  starrocksFeProxySpec:
    # specify the fe proxy deploy or not.
    enabled: true
    replicas: 1
    imagePullPolicy: IfNotPresent
    # default nginx:1.24.0
    image:
      repository: ""
      tag: ""
    resources:
      limits:
        cpu: 1
        memory: 2Gi
      requests:
        cpu: 1
        memory: 2Gi
    # set the resolver for nginx server, default kube-dns.kube-system.svc.cluster.local
    resolver: ""
    service:
      # the fe proxy service type, only supported ClusterIP, NodePort, LoadBalancer
      # default ClusterIP
      type: LoadBalancer
```

## Add the Helm chart

```
helm repo add starrocks-community https://starrocks.github.io/starrocks-kubernetes-operator
helm repo update starrocks-community
helm search repo starrocks-community
```

```bash
NAME                              	CHART VERSION	APP VERSION	DESCRIPTION
starrocks-community/kube-starrocks	1.9.6        	3.2-latest 	kube-starrocks includes two subcharts, operator...
starrocks-community/operator      	1.9.6        	1.9.6      	A Helm chart for StarRocks operator
starrocks-community/starrocks     	1.9.6        	3.2-latest 	A Helm chart for StarRocks cluster
starrocks-community/warehouse     	1.9.6        	3.2-latest 	Warehouse is currently a feature of the StarRoc...
```

### Adjust the resources assigned to the StarRocks deployment

The `values.yaml` file deploys the StarRocks Enterprise Edition operator and a StarRocks deployment. Before running `helm install` look at the file and edit the resources to fit your use-case.
A basic StarRocks system can run with two cores and 4 GB RAM. If you are not running production workloads and are just experimenting with deploying a cluster with the operator tune down the resource requirements in `values.yaml`.

## Deploy the operator and the StarRocks cluster specified in values.yaml

```bash
helm install starrocks starrocks-community/kube-starrocks -f values.yaml
kubectl --namespace default get starrockscluster -l "cluster=kube-starrocks"
```

Deploying the operator is very quick. Deploying the StarRocks cluster can take some time depending on the number of nodes being deployed and whether your Kubernetes environment has reserved resources or needs to scale up. If you created a default GKE cluster with Autopilot, then the nodes will have to be requested and assigned. This will take longer. You can watch the process with `kubectl`.

```bash
kubectl get pods -w
```

:::note
You will see a pod `kube-starrocks-initpwd-<random string>` cycling through error states while it waits for the FE and BE pods to enter the running state so that it can set the initial password for the `root` database user.
:::

## Wait for the pods to become ready

Once the FE and BE pods are showing `1/1` in the `READY` column and `Running` in the `STATUS` column your StarRocks deployment is ready for use.

```bash
kubectl get pods
```

```bash
NAME                                      READY   STATUS      RESTARTS   AGE
kube-starrocks-be-0                       1/1     Running     0          24m
kube-starrocks-fe-0                       1/1     Running     0          28m
kube-starrocks-initpwd-xgqn2              0/1     Completed   4          28m
kube-starrocks-operator-d5557cbbb-dlj4t   1/1     Running     0          28m
```

## Launch a MySQL client within the first FE

```bash
kubectl exec --stdin --tty kube-starrocks-fe-0 -- mysql -h 127.0.0.1 -P 9030 -u root -p
```

Log in with the password you created for the `root` user.

## Verify that SSL is in use

The MySQL client uses SSL by default if the database is configured to use SSL. Run the `status` command at the `mysql>` prompt to verify that your connection is using SSL:

```bash
status
```

```bash
--------------
mysql  Ver 8.0.36-0ubuntu0.22.04.1 for Linux on x86_64 ((Ubuntu))

Connection id:          3
Current database:       'root'@'127.0.0.1'
Current user:           'root'@'127.0.0.1'
-- highlight-next-line
SSL:                    Cipher in use is TLS_AES_256_GCM_SHA384
Current pager:          stdout
Using outfile:          ''
Using delimiter:        ;
Server version:         5.1.0 3.2.6-ee-9880d8d
Protocol version:       10
Connection:             127.0.0.1 via TCP/IP
Server characterset:    utf8
Db     characterset:    utf8
Client characterset:    utf8
Conn.  characterset:    utf8
TCP port:               9030
Binary data as:         Hexadecimal
--------------
```

## Load some data

The rest of this guide uses two public datasets. You will load the data and then query it to answer some questions about driving conditions in New York City.

### Set the number of replicas

StarRocks is designed to work with replicated data for resilience and performance. Because this demo system has only one backend engine (BE) the default replication number has to be reduced from 3 to 1.

Set the default number of replicas to `1` by running this command at the `mysql >` prompt:

```
ADMIN SET FRONTEND CONFIG ("default_replication_num" = "1");
```

### Get the proxy address

Connectivity to the StarRocks cluster from outside of the Kubernetes cluster is needed to load data. 
The FE proxy service provides external connectivity. Get the proxy address and port from the services list. In the output
below the proxy is at `34.176.197.63:8080`.

```bash
kubectl get services
```

```bash
kubectl get services
NAME                              TYPE           CLUSTER-IP      EXTERNAL-IP     PORT(S)                               AGE
kube-starrocks-be-search          ClusterIP      None            <none>          9050/TCP                              29m
kube-starrocks-be-service         ClusterIP      10.71.169.199   <none>          9060/TCP,8040/TCP,9050/TCP,8060/TCP   29m
# highlight-next-line
kube-starrocks-fe-proxy-service   LoadBalancer   10.71.167.95    34.176.197.63   8080:31676/TCP                        29m
kube-starrocks-fe-search          ClusterIP      None            <none>          9030/TCP                              30m
kube-starrocks-fe-service         ClusterIP      10.71.170.35    <none>          8030/TCP,9020/TCP,9030/TCP,9010/TCP   30m
kubernetes                        ClusterIP      10.71.160.1     <none>          443/TCP                               4h32m
```

### Use stream load

NOTE: The docs for loading the datasets and running the queries are in the assets folder and will be reused for this doc. Do not include the following snippets in the published doc.

#### Crash

Use the `EXTERNAL-IP` and port number from the proxy entry of the services list as the curl location. In the services list from the previous step this would be `34.176.197.63:8080`

```bash
curl --location-trusted -u root             \
    -T ./NYPD_Crash_Data.csv                \
    -H "label:crashdata-0"                  \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns:tmp_CRASH_DATE, tmp_CRASH_TIME, CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5" \
    -XPUT http://34.176.197.63:8080/api/quickstart/crashdata/_stream_load
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
    "LoadTimeMs": 2702,
    "BeginTxnTimeMs": 35,
    "StreamLoadPlanTimeMs": 119,
    "ReadDataTimeMs": 1593,
    "WriteDataTimeMs": 2483,
    "CommitAndPublishTimeMs": 64,
    "ErrorURL": "http://kube-starrocks-be-0.kube-starrocks-be-search.default.svc.cluster.local:8040/api/_load_error_log?file=error_log_e54d882bc086d2f9_13be147ea36b6dbc"
}%
```

#### Weather

```bash
    -XPUT http://34.176.197.63:8080/api/quickstart/weatherdata/_stream_load
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
    "LoadTimeMs": 310,
    "BeginTxnTimeMs": 1,
    "StreamLoadPlanTimeMs": 9,
    "ReadDataTimeMs": 110,
    "WriteDataTimeMs": 281,
    "CommitAndPublishTimeMs": 18
}%
```

---

### Connect to StarRocks with a SQL client

:::tip

If you are using a client other than the mysql CLI, open that now.
:::

This command will run the `mysql` command in the Docker container:

```sql
docker exec -it quickstart \
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "
```

---

## Create some tables

<DDL />

---

## Load two datasets

There are many ways to load data into StarRocks. For this tutorial the simplest way is to use curl and StarRocks Stream Load.

:::tip
Open a new shell as these curl commands are run at the operating system prompt, not in the `mysql` client. The commands refer to the datasets that you downloaded, so run them from the directory where you downloaded the files.

You will be prompted for a password. You probably have not assigned a password to the MySQL `root` user, so just hit enter.
:::

The `curl` commands look complex, but they are explained in detail at the end of the tutorial. For now, we recommend running the commands and running some SQL to analyze the data, and then reading about the data loading details at the end.

### New York City collision data - Crashes

```bash
curl --location-trusted -u root             \
    -T ./NYPD_Crash_Data.csv                \
    -H "label:crashdata-0"                  \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns:tmp_CRASH_DATE, tmp_CRASH_TIME, CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5" \
    -XPUT http://localhost:8030/api/quickstart/crashdata/_stream_load
```

Here is the output of the preceding command. The first highlighted section shows what you should expect to see (OK and all but one row inserted). One row was filtered out because it does not contain the correct number of columns.

```bash
Enter host password for user 'root':
{
    "TxnId": 2,
    "Label": "crashdata-0",
    "Status": "Success",
    # highlight-start
    "Message": "OK",
    "NumberTotalRows": 423726,
    "NumberLoadedRows": 423725,
    # highlight-end
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 96227746,
    "LoadTimeMs": 1013,
    "BeginTxnTimeMs": 21,
    "StreamLoadPlanTimeMs": 63,
    "ReadDataTimeMs": 563,
    "WriteDataTimeMs": 870,
    "CommitAndPublishTimeMs": 57,
    # highlight-start
    "ErrorURL": "http://127.0.0.1:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f"
    # highlight-end
}%
```

If there was an error the output provides a URL to see the error messages. Open this in a browser to find out what happened. Expand the detail to see the error message:

<details>

<summary>Reading error messages in the browser</summary>

```bash
Error: Value count does not match column count. Expect 29, but got 32.

Column delimiter: 44,Row delimiter: 10.. Row: 09/06/2015,14:15,,,40.6722269,-74.0110059,"(40.6722269, -74.0110059)",,,"R/O 1 BEARD ST. ( IKEA'S 
09/14/2015,5:30,BRONX,10473,40.814551,-73.8490955,"(40.814551, -73.8490955)",TORRY AVENUE                    ,NORTON AVENUE                   ,,0,0,0,0,0,0,0,0,Driver Inattention/Distraction,Unspecified,,,,3297457,PASSENGER VEHICLE,PASSENGER VEHICLE,,,
```

</details>

### Weather data

Load the weather dataset in the same manner as you loaded the crash data.

```bash
curl --location-trusted -u root             \
    -T ./72505394728.csv                    \
    -H "label:weather-0"                    \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, REPORT_TYPE, SOURCE, HourlyAltimeterSetting, HourlyDewPointTemperature, HourlyDryBulbTemperature, HourlyPrecipitation, HourlyPresentWeatherType, HourlyPressureChange, HourlyPressureTendency, HourlyRelativeHumidity, HourlySkyConditions, HourlySeaLevelPressure, HourlyStationPressure, HourlyVisibility, HourlyWetBulbTemperature, HourlyWindDirection, HourlyWindGustSpeed, HourlyWindSpeed, Sunrise, Sunset, DailyAverageDewPointTemperature, DailyAverageDryBulbTemperature, DailyAverageRelativeHumidity, DailyAverageSeaLevelPressure, DailyAverageStationPressure, DailyAverageWetBulbTemperature, DailyAverageWindSpeed, DailyCoolingDegreeDays, DailyDepartureFromNormalAverageTemperature, DailyHeatingDegreeDays, DailyMaximumDryBulbTemperature, DailyMinimumDryBulbTemperature, DailyPeakWindDirection, DailyPeakWindSpeed, DailyPrecipitation, DailySnowDepth, DailySnowfall, DailySustainedWindDirection, DailySustainedWindSpeed, DailyWeather, MonthlyAverageRH, MonthlyDaysWithGT001Precip, MonthlyDaysWithGT010Precip, MonthlyDaysWithGT32Temp, MonthlyDaysWithGT90Temp, MonthlyDaysWithLT0Temp, MonthlyDaysWithLT32Temp, MonthlyDepartureFromNormalAverageTemperature, MonthlyDepartureFromNormalCoolingDegreeDays, MonthlyDepartureFromNormalHeatingDegreeDays, MonthlyDepartureFromNormalMaximumTemperature, MonthlyDepartureFromNormalMinimumTemperature, MonthlyDepartureFromNormalPrecipitation, MonthlyDewpointTemperature, MonthlyGreatestPrecip, MonthlyGreatestPrecipDate, MonthlyGreatestSnowDepth, MonthlyGreatestSnowDepthDate, MonthlyGreatestSnowfall, MonthlyGreatestSnowfallDate, MonthlyMaxSeaLevelPressureValue, MonthlyMaxSeaLevelPressureValueDate, MonthlyMaxSeaLevelPressureValueTime, MonthlyMaximumTemperature, MonthlyMeanTemperature, MonthlyMinSeaLevelPressureValue, MonthlyMinSeaLevelPressureValueDate, MonthlyMinSeaLevelPressureValueTime, MonthlyMinimumTemperature, MonthlySeaLevelPressure, MonthlyStationPressure, MonthlyTotalLiquidPrecipitation, MonthlyTotalSnowfall, MonthlyWetBulb, AWND, CDSD, CLDD, DSNW, HDSD, HTDD, NormalsCoolingDegreeDay, NormalsHeatingDegreeDay, ShortDurationEndDate005, ShortDurationEndDate010, ShortDurationEndDate015, ShortDurationEndDate020, ShortDurationEndDate030, ShortDurationEndDate045, ShortDurationEndDate060, ShortDurationEndDate080, ShortDurationEndDate100, ShortDurationEndDate120, ShortDurationEndDate150, ShortDurationEndDate180, ShortDurationPrecipitationValue005, ShortDurationPrecipitationValue010, ShortDurationPrecipitationValue015, ShortDurationPrecipitationValue020, ShortDurationPrecipitationValue030, ShortDurationPrecipitationValue045, ShortDurationPrecipitationValue060, ShortDurationPrecipitationValue080, ShortDurationPrecipitationValue100, ShortDurationPrecipitationValue120, ShortDurationPrecipitationValue150, ShortDurationPrecipitationValue180, REM, BackupDirection, BackupDistance, BackupDistanceUnit, BackupElements, BackupElevation, BackupEquipment, BackupLatitude, BackupLongitude, BackupName, WindEquipmentChangeDate" \
    -XPUT http://localhost:8030/api/quickstart/weatherdata/_stream_load
```

---

## Answer some questions

<SQL />

---

## Summary

In this tutorial you:

- Deployed StarRocks in Docker
- Loaded crash data provided by New York City and weather data provided by NOAA
- Analyzed the data using SQL JOINs to find out that driving in low visibility or icy streets is a bad idea

There is more to learn; we intentionally glossed over the data transformation done during the Stream Load. The details on that are in the notes on the curl commands below.

---

## Notes on the curl commands

<Curl />

---

## More information

[StarRocks table design](../table_design/StarRocks_table_design.md)

[Materialized views](../cover_pages/mv_use_cases.mdx)

[Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)

The [Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) dataset is provided by New York City subject to these [terms of use](https://www.nyc.gov/home/terms-of-use.page) and [privacy policy](https://www.nyc.gov/home/privacy-policy.page).

The [Local Climatological Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD) is provided by NOAA with this [disclaimer](https://www.noaa.gov/disclaimer) and this [privacy policy](https://www.noaa.gov/protecting-your-privacy).
## Cleanup

```bash
# helm uninstall starrocks
```
