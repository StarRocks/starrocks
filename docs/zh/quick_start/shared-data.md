---
displayed_sidebar: "Chinese"
sidebar_position: 2
description: "存算分离"
---

# 存算分离

import DDL from '../assets/quick-start/_DDL.mdx'
import Clients from '../assets/quick-start/_clientsCompose.mdx'
import SQL from '../assets/quick-start/_SQL.mdx'
import Curl from '../assets/quick-start/_curl.mdx'


在 StarRocks 存算分离架构中，数据存储在成本更低且更可靠的远程存储系统中，例如 Amazon S3、Google Cloud Storage、Azure Blob Storage 以及其他支持 S3 协议的存储，如 MinIO。热数据会被缓存到本地，在查询命中缓存的前提下，存算分离集群的查询性能与存算一体集群相当。同时，在存算分离架构下，计算节点（CN）可以在几秒内根据需要实现扩缩容。因此，StarRocks 的存算分离架构不仅降低了存储成本，保证了资源隔离性能，还提供了计算资源的弹性和可扩展性。

本教程涵盖以下内容：

- 使用 Docker Compose 部署 StarRocks 存算分离集群以及 MinIO 作为对象存储。
- 导入数据集，并在导入过程中进行基本的数据转换。
- 查询分析数据。

本教程中使用的数据由 NYC OpenData 和 NOAA 的 National Centers for Environmental Information 提供。教程仅截取了数据集的部分字段。

---

## 前提条件

### Docker

- [安装 Docker](https://docs.docker.com/engine/install/)。
- 为 Docker 分配 4 GB RAM。
- 为 Docker 分配 10 GB 的空闲磁盘空间。

### SQL 客户端

您可以使用 Docker 环境中提供的 MySQL Client，也可以使用其他兼容 MySQL 的客户端，包括本教程中涉及的 DBeaver 和 MySQL Workbench。

### curl

`curl` 命令用于向 StarRocks 中导入数据以及下载数据集。您可以通过在终端运行 `curl` 或 `curl.exe` 来检查您的操作系统是否已安装 curl。如果未安装 curl，[请点击此处获取 curl](https://curl.se/dlwiz/?type=bin)。

---

## 术语

### FE

FE 节点负责元数据管理、客户端连接管理、查询计划和查询调度。每个 FE 在其内存中存储和维护完整的元数据副本，确保每个 FE 都能提供无差别的服务。

### CN

CN 节点在**存算分离**或**存算一体**集群中负责执行查询。

### BE

BE 节点在**存算一体**集群中负责数据存储和执行查询。

:::note
当前教程不包含 BE 节点，以上内容仅供您了解 BE 和 CN 之间的区别。
:::

---

## 启动 StarRocks

要基于对象存储部署 StarRocks 存算分离集群，您需要部署以下服务：

- 一个 FE 节点
- 一个 CN 节点
- 对象存储

本教程使用 MinIO 作为对象存储。

StarRocks 提供了一个 Docker Compose 文件，用于搭建包含以上三个必要容器的环境。

下载 Docker Compose 文件。

```bash
mkdir quickstart
cd quickstart
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml
```

启动容器。

```bash
docker compose up -d
```

## 检查环境状态

成功启动后，FE 和 CN 节点大约需要 30 秒才能部署完成。需要通过 `docker compose ps` 命令检查服务的运行状态，直到 `starrocks-fe` 和 `starrocks-cn` 的状态变为 `healthy`。

```bash
docker compose ps
```

返回：

```plaintext
SERVICE        CREATED          STATUS                    PORTS
starrocks-cn   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8040->8040/tcp
starrocks-fe   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8030->8030/tcp, 0.0.0.0:9020->9020/tcp, 0.0.0.0:9030->9030/tcp
minio          25 seconds ago   Up 24 seconds             0.0.0.0:9000-9001->9000-9001/tcp
```

以上返回不会显示 MinIO 容器的健康状态，您需要在下一步中通过使用 MinIO Web UI 验证其健康状态。

---

## 生成 MinIO 认证凭证

要访问 MinIO，您需要生成一个 **访问密钥**。

### 打开 MinIO Web UI

从浏览器进入 `http://localhost:9001/access-keys`。登录用的用户名和密码已经在 Docker Compose 文件中指定，分别为 `minioadmin` 和 `minioadmin`。成功登录后，点击 **Create access key +** 创建密钥。

MinIO 将生成一对密钥，点击 **Create** 生成并下载密钥。

![点击 Create](../assets/quick-start/MinIO-create.png)

:::note
系统不会自动保存密钥，所以请确认保存密钥后再离开页面。
:::

---

## SQL 客户端

<Clients />

---

## 下载数据

将教程所需的数据集下载到 FE 容器中。

在容器中启动 Shell。

```bash
docker compose exec starrocks-fe bash
```

创建用于存放数据集文件的路径。

```bash
mkdir quickstart
cd quickstart
```

下载交通事故数据集。

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

下载天气数据集。

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## 配置 StarRocks 存算分离集群

启动 StarRocks 以及 MinIO 后，您需要连接并配置 StarRocks 存算分离集群。

### 使用 SQL 客户端连接到 StarRocks

- 如果您使用 StarRocks 容器中的 MySQL Client，需要从包含 `docker-compose.yml` 文件的路径运行以下命令。

  ```sql
  docker compose exec starrocks-fe \
  mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
  ```

- 如果您使用其他客户端，请打开客户端并连接至 StarRocks。

### 创建存储卷

StarRocks 存算分离集群需要通过存储卷将数据持久化到对象存储中。

以下为创建存储卷时需要用到的信息：

- MinIO 服务器 URL 为 `http://minio:9000`。
- 集群中的数据将通过存储卷持久化在 `starrocks` 存储桶中的 `shared` 的文件夹下。请注意，`shared` 文件夹将在数据第一次导入集群时自动创建。
- MinIO 服务器未使用 SSL。
- 连接 MinIO 需要 Access Key 和 Secret Key 即为 MinIO Web UI 中创建的访问密钥。
- 您需要将该存储卷设置为默认存储卷。

:::tip
运行以下命令之前，您需要将 `aws.s3.access_key` 和 `aws.s3.secret_key` 配置项的值替换为您在 MinIO 中创建的 Access Key 和 Secret Key。
:::

```bash
CREATE STORAGE VOLUME shared
TYPE = S3
LOCATIONS = ("s3://starrocks/shared/")
PROPERTIES
(
    "enabled" = "true",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.use_aws_sdk_default_behavior" = "false",
    "aws.s3.enable_ssl" = "false",
    "aws.s3.use_instance_profile" = "false",
    # highlight-start
    "aws.s3.access_key" = "AAAAAAAAAAAAAAA",
    "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
    # highlight-end
);
SET shared AS DEFAULT STORAGE VOLUME;
```

创建成功后，查看该存储卷。

```sql
DESC STORAGE VOLUME shared\G
```

:::tip
`\G` 表示将查询结果按列打印。如果您的 SQL 客户端不支持垂直格式输出，则需要将 `\G` 替换为 `;`。
:::

返回：

```plaintext
*************************** 1. row ***************************
     Name: shared
     Type: S3
IsDefault: true
# highlight-start
 Location: s3://starrocks/shared/
   Params: {"aws.s3.access_key":"******","aws.s3.secret_key":"******","aws.s3.endpoint":"http://minio:9000","aws.s3.region":"us-east-1","aws.s3.use_instance_profile":"false","aws.s3.use_aws_sdk_default_behavior":"false"}
# highlight-end
  Enabled: true
  Comment:
1 row in set (0.03 sec)
```

---

## 建表

<DDL />

---

## 导入数据集

StarRocks 提供多种数据导入方法。本教程使用 curl 命令发起 Stream Load 任务导入数据。

Stream Load 使用的 curl 命令虽然看起来复杂，但本教程在最后部分提供了详细解释。建议您先运行该命令导入数据，然后在教程结束后了解有关数据导入的详细内容。

### 导入纽约市交通事故数据

通过 FE 容器的 Shell Session 进入数据集文件所在的路径，然后运行以下命令。

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

运行命令后，系统会提示您输入密码。由于您尚未为 `root` 用户分配密码，所以只需按 **Enter** 键跳过。

以下是上述命令的返回。其中 `Status` 为 `Success`，表示导入成功。`NumberFilteredRows` 为 1，表示数据集中包含一行错误数据，StarRocks 在导入过程将该行过滤。

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
    "ErrorURL": "http://10.5.0.3:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f"
    # highlight-end
}%
```

当 Stream Load 发生错误时，StarRocks 会返回一个包含错误信息的 URL。由于容器具有私有 IP 地址，您必须通过容器中的 Shell Session 运行 curl 命令查看。

```bash
curl http://10.5.0.3:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f
```

点击下方 **错误信息** 查看 URL 中包含的错误信息。

<details>

<summary>错误信息</summary>

```bash
Error: Value count does not match column count. Expect 29, but got 32.
Column delimiter: 44,Row delimiter: 10.. Row: 09/06/2015,14:15,,,40.6722269,-74.0110059,"(40.6722269, -74.0110059)",,,"R/O 1 BEARD ST. ( IKEA'S 
09/14/2015,5:30,BRONX,10473,40.814551,-73.8490955,"(40.814551, -73.8490955)",TORRY AVENUE                    ,NORTON AVENUE                   ,,0,0,0,0,0,0,0,0,Driver Inattention/Distraction,Unspecified,,,,3297457,PASSENGER VEHICLE,PASSENGER VEHICLE,,,
```

</details>

### 导入天气数据

依照同样的方式导入天气数据。

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

## 验证数据是否存储在 MinIO 中

打开 [MinIO Web UI](http://localhost:9001/browser/starrocks/) 并验证在 `starrocks/shared/` 下的每个路径中是否有 `data`、`metadata` 和 `schema` 路径。

:::tip
在导入数据时，`starrocks/shared/` 下的文件夹名称是动态生成的。您会在 `shared` 下面看到一个路径，然后在该路径下看到另外两个路径。在每个路径内，您都能看到 `data`、`metadata` 和 `schema` 路径。

![MinIO Web UI](../assets/quick-start/MinIO-data.png)
:::

---

## 查询数据并回答问题

<SQL />

---

## 存算分离集群配置细节

本小节介绍 StarRocks 存算分离集群的配置信息。

### CN 配置

本教程中使用的 CN 配置是默认配置。如下所示。您无需进行任何更改。

```bash
sys_log_level = INFO

be_port = 9060
be_http_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060
```

### FE 配置

FE 配置与默认配置略有不同，需要额外添加与对象存储相关的配置项。

以下为 `docker-compose.yml` 文件中用于生成 FE 配置的命令。

```yml
    command: >
      bash -c "echo run_mode=shared_data >> /opt/starrocks/fe/conf/fe.conf &&
      echo cloud_native_meta_port=6090 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_path=starrocks >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_endpoint=minio:9000 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_use_instance_profile=false >> /opt/starrocks/fe/conf/fe.conf &&
      echo cloud_native_storage_type=S3 >> /opt/starrocks/fe/conf/fe.conf &&
      echo aws_s3_use_aws_sdk_default_behavior=false >> /opt/starrocks/fe/conf/fe.conf &&
      sh /opt/starrocks/fe/bin/start_fe.sh"
```

以上命令会生成以下 FE 配置文件 **fe.conf**：

```bash title='fe/fe.conf'
LOG_DIR = ${STARROCKS_HOME}/log

DATE = "$(date +%Y%m%d-%H%M%S)"
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:${LOG_DIR}/fe.gc.log.$DATE -XX:+PrintConcurrentLocks"

JAVA_OPTS_FOR_JDK_11="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time"

sys_log_level = INFO

http_port = 8030
rpc_port = 9020
query_port = 9030
edit_log_port = 9010
mysql_service_nio_enabled = true

# highlight-start
run_mode=shared_data
aws_s3_path=starrocks
aws_s3_endpoint=minio:9000
aws_s3_use_instance_profile=false
cloud_native_storage_type=S3
aws_s3_use_aws_sdk_default_behavior=false
# highlight-end
```

参数的具体解释如下：

#### `run_mode=shared_data`

该参数用于启用存算分离模式。

#### `aws_s3_path=starrocks`

存储桶名称。

#### `aws_s3_endpoint=minio:9000`

MinIO 端点，包括端口号。

#### `aws_s3_use_instance_profile=false`

是否使用 Instance Profile 作为凭证。因为您需要使用访问密钥连接 MinIO，需要该项设置为 false。

#### `cloud_native_storage_type=S3`

用于指定对象存储类型。对于 MinIO，需要将该项设置为 S3。

#### `aws_s3_use_aws_sdk_default_behavior=false`

是否使用 AWS S3 默认 SDK 认证方式。在使用 MinIO 时，始终将此参数设置为 false。

---

## 总结

在本教程中，您完成了以下目标：

- 在 Docker 中部署了 StarRocks 和 MinIO
- 创建了 MinIO 访问密钥
- 配置了使用 MinIO 的 StarRocks 存储卷
- 向 StarRocks 导入了纽约市的交通事故数据和天气数据
- 使用 JOIN 查询分析数据

除以上内容外，本教程有意略过了在 Stream Load 命令中数据转换相关的参数说明。关于此内容的详细信息在下一小节中提供。

## 关于 curl 命令的注释

<Curl />

## 更多信息

- [StarRocks 表设计](../table_design/StarRocks_table_design.md)
- [物化视图](../cover_pages/mv_use_cases.mdx)
- [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)
- [纽约市交通事故数据](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) 数据集由纽约市提供，受到以下[使用条款](https://www.nyc.gov/home/terms-of-use.page)和[隐私政策](https://www.nyc.gov/home/privacy-policy.page)的约束。
- [气象数据](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD)由 NOAA 提供，附带[免责声明](https://www.noaa.gov/disclaimer)和[隐私政策](https://www.noaa.gov/protecting-your-privacy)。
