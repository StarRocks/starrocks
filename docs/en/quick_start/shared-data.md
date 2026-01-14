---
displayed_sidebar: docs
sidebar_position: 2
description: Separate compute and storage
---

# Separate storage and compute

import DDL from '../_assets/quick-start/_DDL.mdx'
import Clients from '../_assets/quick-start/_clientsCompose.mdx'
import SQL from '../_assets/quick-start/_SQL.mdx'
import Curl from '../_assets/quick-start/_curl.mdx'

In systems that separate storage from compute data is stored in low-cost reliable remote storage systems such as Amazon S3, Google Cloud Storage, Azure Blob Storage, and other S3-compatible storage like MinIO. Hot data is cached locally and When the cache is hit, the query performance is comparable to that of storage-compute coupled architecture. Compute nodes (CN) can be added or removed on demand within seconds. This architecture reduces storage cost, ensures better resource isolation, and provides elasticity and scalability.

This tutorial covers:

- Running StarRocks in Docker containers
- Using MinIO for Object Storage
- Configuring StarRocks for shared-data
- Loading two public datasets
- Analyzing the data with SELECT and JOIN
- Basic data transformation (the **T** in ETL)

The data used is provided by NYC OpenData and the National Centers for Environmental Information at NOAA.

Both of these datasets are very large, and because this tutorial is intended to help you get exposed to working with StarRocks we are not going to load data for the past 120 years. You can run the Docker image and load this data on a machine with 4 GB RAM assigned to Docker. For larger fault-tolerant and scalable deployments we have other documentation and will provide that later.

There is a lot of information in this document, and it is presented with the step by step content at the beginning, and the technical details at the end. This is done to serve these purposes in this order:

1. Allow the reader to load data in a shared-data deployment and analyze that data.
2. Provide the configuration details for shared-data deployments.
3. Explain the basics of data transformation during loading.

---

## Prerequisites

### Docker

- [Docker](https://docs.docker.com/engine/install/)
- 4 GB RAM assigned to Docker
- 10 GB free disk space assigned to Docker

### SQL client

You can use the SQL client provided in the Docker environment, or use one on your system. Many MySQL compatible clients will work, and this guide covers the configuration of DBeaver and MySQL Workbench.

### curl

`curl` is used to issue the data load job to StarRocks, and to download the datasets. Check to see if you have it installed by running `curl` or `curl.exe` at your OS prompt. If curl is not installed, [get curl here](https://curl.se/dlwiz/?type=bin).

### `/etc/hosts`

The ingest method used in this guide is Stream Load. Stream Load connects to the FE service to start the ingest job. The FE then assigns the job to a backend node, the CN in this guide. In order for the ingest job to connect to the CN the name of the CN must be available to your operating system. Add this line to `/etc/hosts`:

```bash
127.0.0.1 starrocks-cn
```

---

## Terminology

### FE

Frontend nodes are responsible for metadata management, client connection management, query planning, and query scheduling. Each FE stores and maintains a complete copy of metadata in its memory, which guarantees indiscriminate services among the FEs.

### CN

Compute Nodes are responsible for executing query plans in shared-data deployments.

### BE

Backend nodes are responsible for both data storage and executing query plans in shared-nothing deployments.

:::note
This guide does not use BEs, this information is included here so that you understand the difference between BEs and CNs.
:::

---

## Edit your hosts file

The ingest method used in this guide is Stream Load. Stream Load connects to the FE service to start the ingest job. The FE then assigns the job to a backend node—the CN in this guide. In order for the ingest job to connect to the CN, the name of the CN must be available to your operating system. Add this line to `/etc/hosts`:

```bash
127.0.0.1 starrocks-cn
```

## Download the lab files

There are three files to download:

- The Docker Compose file that deploys the StarRocks and MinIO environment
- New York City crash data
- Weather data

This guide uses MinIO, which is S3 compatible Object Storage provided under the GNU Affero General Public License.

### Create a directory to store the lab files

```bash
mkdir quickstart
cd quickstart
```

### Download the Docker Compose file

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml
```

### Download the data

Download these two datasets:

#### New York City crash data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

#### Weather data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## Deploy StarRocks and MinIO

```bash
docker compose up --detach --wait --wait-timeout 120
```

It should take around 30 seconds for the FE, CN, and MinIO services to become healthy. The `quickstart-minio_mc-1` container will show a status of `Waiting` and also an exit code. An exit code of `0` indicates success.

```bash
[+] Running 4/5
 ✔ Network quickstart_default       Created    0.0s
 ✔ Container minio                  Healthy    6.8s
 ✔ Container starrocks-fe           Healthy    29.3s
 ⠼ Container quickstart-minio_mc-1  Waiting    29.3s
 ✔ Container starrocks-cn           Healthy    29.2s
container quickstart-minio_mc-1 exited (0)
```

---

## MinIO

This quick start uses MinIO for shared storage.

### Verify the MinIO credentials

To use MinIO for Object Storage with StarRocks, StarRocks needs a MinIO access key. The access key was generated during the startup of the Docker services. To help you better understand the way that StarRocks connects to MinIO you should verify that the key exists.

Browse to [http://localhost:9001/access-keys](http://localhost:9001/access-keys) The username and password are specified in the Docker compose file, and are `miniouser` and `miniopassword`. You should see that there is one access key. The Key is `AAAAAAAAAAAAAAAAAAAA`, you cannot see the secret in the MinIO Console, but it is in the Docker compose file and is `BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB`:

![View the MinIO access key](../_assets/quick-start/MinIO-view-key.png)

:::tip
If there are no access keys showing in the MinIO web UI, check the logs of the `minio_mc` service:

```bash
docker compose logs minio_mc
```

Try rerunning the `minio_mc` pod:

```bash
docker compose run minio_mc
```
:::

### Create a bucket for your data

When you create a storage volume in StarRocks you will specify the `LOCATION` for the data:

```sh
    LOCATIONS = ("s3://my-starrocks-bucket/")
```

Open [http://localhost:9001/buckets](http://localhost:9001/buckets) and add a bucket for the storage volume. Name the bucket `my-starrocks-bucket`. Accept the defaults for the three listed options.

---

## SQL Clients

<Clients />

---


## StarRocks configuration for shared-data

At this point you have StarRocks running, and you have MinIO running. The MinIO access key is used to connect StarRocks and Minio.

This is the part of the `FE` configuration that specifies that the StarRocks deployment will use shared data. This was added to the file `fe.conf` when Docker Compose created the deployment.

```sh
# enable the shared data run mode
run_mode = shared_data
cloud_native_storage_type = S3
```

:::info
You can verify these settings by running this command from the `quickstart` directory and looking at the end of the file: 
:::

```sh
docker compose exec starrocks-fe \
  cat /opt/starrocks/fe/conf/fe.conf
```
:::

### Connect to StarRocks with a SQL client

:::tip

Run this command from the directory containing the `docker-compose.yml` file.

If you are using a client other than the MySQL Command-Line Client, open that now.
:::

```sql
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

#### Examine the storage volumes


```sql
SHOW STORAGE VOLUMES;
```

:::tip
There should be no storage volumes, you will create one next.
:::

```sh
Empty set (0.04 sec)
```

#### Create a shared-data storage volume

Earlier you created a bucket in MinIO named `my-starrocks-volume`, and you verified that MinIO has an access key named `AAAAAAAAAAAAAAAAAAAA`. The following SQL will create a storage volume in the MionIO bucket using the access key and secret.

```sql
CREATE STORAGE VOLUME s3_volume
    TYPE = S3
    LOCATIONS = ("s3://my-starrocks-bucket/")
    PROPERTIES
    (
         "enabled" = "true",
         "aws.s3.endpoint" = "minio:9000",
         "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
         "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
         "aws.s3.use_instance_profile" = "false",
         "aws.s3.use_aws_sdk_default_behavior" = "false"
     );
```

Now you should see a storage volume listed, earlier it was an empty set:

```
SHOW STORAGE VOLUMES;
```

```
+----------------+
| Storage Volume |
+----------------+
| s3_volume      |
+----------------+
1 row in set (0.02 sec)
```

View the details of the storage volume and note that this is nott yet the default volume, and that it is configured to use your bucket:

```
DESC STORAGE VOLUME s3_volume\G
```

:::tip
Some of the SQL in this document, and many other documents in the StarRocks documentation, and with `\G` instead
of a semicolon. The `\G` causes the mysql CLI to render the query results vertically.

Many SQL clients do not interpret vertical formatting output, so you should replace `\G` with `;`.
:::

```sh
*************************** 1. row ***************************
     Name: s3_volume
     Type: S3
# highlight-start
IsDefault: false
 Location: s3://my-starrocks-bucket/
# highlight-end
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_web_identity_token_file":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: true
  Comment:
1 row in set (0.02 sec)
```

## Set the default storage volume

```
SET s3_volume AS DEFAULT STORAGE VOLUME;
```

```
DESC STORAGE VOLUME s3_volume\G
```

```sh
*************************** 1. row ***************************
     Name: s3_volume
     Type: S3
# highlight-next-line
IsDefault: true
 Location: s3://my-starrocks-bucket/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_web_identity_token_file":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: true
  Comment:
1 row in set (0.02 sec)
```

## Create a database

```
CREATE DATABASE IF NOT EXISTS quickstart;
```

Verify that the database `quickstart` is using the storage volume `s3_volume`:

```
SHOW CREATE DATABASE quickstart \G
```

```sh
*************************** 1. row ***************************
       Database: quickstart
Create Database: CREATE DATABASE `quickstart`
# highlight-next-line
PROPERTIES ("storage_volume" = "s3_volume")
```

---

## Create some tables

<DDL />

---

## Load two datasets

There are many ways to load data into StarRocks. For this tutorial the simplest way is to use curl and StarRocks Stream Load.

:::tip

Run these curl commands from the directory where you downloaded the dataset.

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

Here is the output of the above command. The first highlighted section shown what you should expect to see (OK and all but one row inserted). One row was filtered out because it does not contain the correct number of columns.

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
    "ErrorURL": "http://starrocks-cn:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f"
    # highlight-end
}%
```

If there was an error the output provides a URL to see the error messages. The error message also contains the backend node that the Stream Load job was assigned to (`starrocks-cn`). Because you added an entry for `starrocks-cn` to the `/etc/hosts` file, you should be able to navigate to it and read the error message.

Expand the summary for the content seen while developing this tutorial:

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

## Verify that data is stored in MinIO

Open MinIO [http://localhost:9001/browser/my-starrocks-bucket](http://localhost:9001/browser/my-starrocks-bucket) and verify that you have entries below `my-starrocks-bucket/`

:::tip
The folder names below `my-starrocks-bucket/` are generated when you load the data. You should see a single directory below `my-starrocks-bucket`, and then two more below that. In those directories you will find the data, metadata, or schema entries.

![MinIO object browser](../_assets/quick-start/MinIO-data.png)
:::

---

## Answer some questions

<SQL />

---

## Configuring StarRocks for shared-data

Now that you have experienced using StarRocks with shared-data it is important to understand the configuration. 

### CN configuration

The CN configuration used here is the default, as the CN is designed for shared-data use. The default configuration is shown below. You do not need to make any changes.

```bash
sys_log_level = INFO

# ports for admin, web, heartbeat service
be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
starlet_port = 9070
```

### FE configuration

The FE configuration is slightly different from the default as the FE must be configured to expect that data is stored in Object Storage rather than on local disks on BE nodes.

The `docker-compose.yml` file generates the FE configuration in the `command`.

```plaintext
# enable shared data, set storage type, set endpoint
run_mode = shared_data
cloud_native_storage_type = S3
```

:::note
This config file does not contain the default entries for an FE, only the shared-data configuration is shown.
:::

The non-default FE configuration settings:

:::note
Many configuration parameters are prefixed with `s3_`. This prefix is used for all Amazon S3 compatible storage types (for example: S3, GCS, and MinIO). When using Azure Blob Storage the prefix is `azure_`.
:::

#### `run_mode=shared_data`

This enables shared-data use.

#### `cloud_native_storage_type=S3`

This specifies whether S3 compatible storage or Azure Blob Storage is used. For MinIO this is always S3.

### Details of `CREATE storage volume`

```sql
CREATE STORAGE VOLUME s3_volume
    TYPE = S3
    LOCATIONS = ("s3://my-starrocks-bucket/")
    PROPERTIES
    (
         "enabled" = "true",
         "aws.s3.endpoint" = "minio:9000",
         "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
         "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
         "aws.s3.use_instance_profile" = "false",
         "aws.s3.use_aws_sdk_default_behavior" = "false"
     );
```

#### `aws_s3_endpoint=minio:9000`

The MinIO endpoint, including port number.

#### `aws_s3_path=starrocks`

The bucket name.

#### `aws_s3_access_key=AAAAAAAAAAAAAAAAAAAA`

The MinIO access key.

#### `aws_s3_secret_key=BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB`

The MinIO access key secret.

#### `aws_s3_use_instance_profile=false`

When using MinIO an access key is used, and so instance profiles are not used with MinIO.

#### `aws_s3_use_aws_sdk_default_behavior=false`

When using MinIO this parameter is always set to false.

### Configuring FQDN mode

The command to start the FE is also changed. The FE service command in the Docker Compose file has the option `--host_type FQDN` added. By setting `host_type` to `FQDN` the Stream Load job is forwarded to the fully qualified domain name of the CN pod, rather than the IP address. This is done because the IP address is in a range assigned to the Docker environment, and is not typically available from the host machine.

These three changes allow traffic between the host network and the CN:

- setting `--host_type` to `FQDN`
- exposing the CN port 8040 to the host network
- adding an entry to the hosts file for `starrocks-cn` pointing to `127.0.0.1`

---

## Summary

In this tutorial you:

- Deployed StarRocks and Minio in Docker
- Created a MinIO access key
- Configured a StarRocks Storage Volume that uses MinIO
- Loaded crash data provided by New York City and weather data provided by NOAA
- Analyzed the data using SQL JOINs to find out that driving in low visibility or icy streets is a bad idea

There is more to learn; we intentionally glossed over the data transform done during the Stream Load. The details on that are in the notes on the curl commands below.

## Notes on the curl commands

<Curl />

## More information

[StarRocks table design](../table_design/StarRocks_table_design.md)

[Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)

The [Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) dataset is provided by New York City subject to these [terms of use](https://www.nyc.gov/home/terms-of-use.page) and [privacy policy](https://www.nyc.gov/home/privacy-policy.page).

The [Local Climatological Data](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD) is provided by NOAA with this [disclaimer](https://www.noaa.gov/disclaimer) and this [privacy policy](https://www.noaa.gov/protecting-your-privacy).
