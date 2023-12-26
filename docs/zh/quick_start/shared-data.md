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


在存算分离架构中，数据存储在成本更低且更可靠的远程存储系统中，例如 Amazon S3、Google Cloud Storage、Azure Blob Storage 以及其他支持 S3 协议的存储，如 MinIO。热数据被缓存到本地，当缓存命中时，查询性能与存算一体架构相当。计算节点（CN）可以在几秒内根据需要实现扩缩容。存算分离架构降低了存储成本，确保更好的资源隔离，并提供了弹性和可扩展性。

本教程涵盖以下内容：

- 在 Docker 容器中运行 StarRocks
- 使用 MinIO 作为对象存储
- 配置 StarRocks 存算分离集群
- 导入两个公共数据集
- 使用 SELECT 和 JOIN 分析数据
- 基本数据转换（ETL 中的 **T**）

以下使用的数据由 NYC OpenData 和 NOAA 的 National Centers for Environmental Information 提供。

由于这两个数据集都非常庞大，而本教程旨在帮助您熟悉使用 StarRocks，因此只会使用数据集中的部分数据。您可以在分配了 4GB RAM 的机器上运行 Docker 镜像并导入这些数据。关于高可用和可扩展的部署，请参考部署章节。

本文档包含大量信息，将以操作步骤开始，技术细节在最后呈现。此举是为了按顺序实现以下目的：

1. 指导读者在存算分离部署中导入数据并分析该数据。
2. 提供存算分离部署的配置详细信息。
3. 解释导入期间数据转换的基础知识。

---

## 前提条件

### Docker

- [Docker](https://docs.docker.com/engine/install/)
- 为 Docker 分配 4 GB RAM
- 为 Docker 分配 10 GB 空闲磁盘空间

### SQL 客户端

您可以使用 Docker 环境中提供的 SQL 客户端，也可以使用系统上的客户端。多数与 MySQL 兼容的客户端都可以使用，包括本教程中涵盖的 DBeaver 和 MySQL WorkBench。

### curl

`curl` 命令用于向 StarRocks 发起数据导入作业，也将用于下载数据集。您可以通过在操作系统的终端运行 `curl` 或 `curl.exe` 来检查是否已安装 curl。如果未安装 curl，[点击此处获取 curl](https://curl.se/dlwiz/?type=bin)。

---

## 术语

### FE
前端引擎（Frontend Engine）。FE 节点负责元数据管理、客户端连接管理、查询计划和查询调度。每个 FE 在其内存中存储和维护完整的元数据副本，这确保了每个 FE 都能提供无差别的服务。

### CN
计算节点（Compute Node）。CN 节点负责在**存算分离**或**存算一体**集群中负责执行查询。

### BE
后端引擎（Backend Engine）。BE 节点在**存算一体**集群中负责数据存储和执行查询。

:::note
当前教程不包含 BE 节点，以上内容仅供您了解 BE 和 CN 之间的区别。
:::

---

## 启动 StarRocks

要基于对象存储搭建 StarRocks 存算分离集群，您需要：

- 一个前端引擎（FE）
- 一个计算节点（CN）
- 对象存储

本教程使用 MinIO 作为对象存储。MinIO 是在 GNU Affero 通用公共许可证下提供的与 S3 兼容的对象存储。

StarRocks 提供了一个 Docker Compose 文件，用以搭建包含以上三个必要容器的环境。

```bash
mkdir quickstart
cd quickstart
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/docker-compose.yml
```

```bash
docker compose up -d
```

运行成功后，需要检查服务的进度。FE 和 CN 大约需要 30 秒才能变为 `healthy` 状态。MinIO 容器不会显示健康状态，您可以使用 MinIO Web UI 验证其健康状态。

运行 `docker compose ps`，直到 FE 和 CN 显示 `healthy` 状态：

```bash
docker compose ps
```

```plaintext
SERVICE        CREATED          STATUS                    PORTS
starrocks-cn   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8040->8040/tcp
starrocks-fe   25 seconds ago   Up 24 seconds (healthy)   0.0.0.0:8030->8030/tcp, 0.0.0.0:9020->9020/tcp, 0.0.0.0:9030->9030/tcp
minio          25 seconds ago   Up 24 seconds             0.0.0.0:9000-9001->9000-9001/tcp
```

---

## 生成 MinIO 凭据

为了在 StarRocks 中使用 MinIO 作为对象存储，您需要生成一个 **访问密钥**。

### 打开 MinIO Web UI

从浏览器中进入 `http://localhost:9001/access-keys`。登录用的用户名和密码在 Docker Compose 文件中已经指定，分别为 `minioadmin` 和 `minioadmin`。此时您会发现尚无访问密钥。点击 **Create access key +**。

MinIO 将生成一个密钥，点击 **Create** 并下载该密钥。

![点击 Create](../assets/quick-start/MinIO-create.png)

:::note
在点击 **Create** 之前，系统不会保存访问密钥，所以请不要仅仅复制密钥然后离开页面。
:::

---

## SQL 客户端

<Clients />

---

## 下载数据

下载教程所需的数据集到您的 FE 容器中。

### 在 FE 容器上打开一个 shell

打开一个 shell 并创建一个用于下载文件的路径：

```bash
docker compose exec starrocks-fe bash
```

```bash
mkdir quickstart
cd quickstart
```

### 下载纽约市交通事故数据

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

### 下载天气数据

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## 配置 StarRocks 存算分离集群

此时，StarRocks 已启动，并且 MinIO 也在运行。MinIO 访问密钥将用于连接 StarRocks 和 MinIo。

### 使用 SQL 客户端连接到 StarRocks

:::tip

从包含 `docker-compose.yml` 文件的路径运行以下命令。

如果您使用的是 mysql CLI 以外的客户端，请打开该客户端。
:::

```sql
docker compose exec starrocks-fe \
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

### 创建 Storage Volume

以下是所示配置的详细信息：

- MinIO 服务器可通过 URL `http://minio:9000` 访问。
- 该 Storage Volume 创建了名为 `starrocks` 的存储桶。
- 写入当前 Storage Volume 的数据将存储在 `starrocks` 桶中名为 `shared` 的文件夹中。
:::tip
`shared` 文件夹将在数据第一次写入当前 Storage Volume 时自动创建。
:::
- MinIO 服务器未使用 SSL。
- MinIO 密钥和密码分别用于 `aws.s3.access_key` 和 `aws.s3.secret_key` 项。请使用您在 MinIO Web UI 中创建的访问密钥。
- 设置 `shared` 为默认 Storage Volume。

:::tip
您需要在运行之前编辑以下命令，并将突出显示的访问密钥信息替换为您在 MinIO 中创建的访问密钥和密码。
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
    "aws.s3.access_key" = "IA2UYcx3Wakpm6sHoFcl",
    "aws.s3.secret_key" = "E33cdRM9MfWpP2FiRpc056Zclg6CntXWa3WPBNMy"
    # highlight-end
);
SET shared AS DEFAULT STORAGE VOLUME;
```

```sql
DESC STORAGE VOLUME shared\G
```

:::tip
本文档中的一些 SQL 语句，以及 StarRocks 的许多其他文档，使用 `\G` 而不是分号 `;` 结束语句。`\G` 用于指示 mysql CLI 在垂直方向上呈现查询结果。

但由于许多 SQL 客户端不支持垂直格式输出，因此您需要将 `\G` 替换为 `;`。
:::

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

:::note
在将数据写入存储桶之前，`shared` 文件夹在 MinIO 对象列表中不可见。
:::

---

## 建表

<DDL />

---

## 导入数据集

StarRocks 有多种数据导入方法。对于本教程，最简单的方法是使用 curl 命令发起 Stream Load 任务导入数据。

:::tip

在存储数据集的路径中，通过 FE shell 运行以下 curl 命令。

系统会提示您输入密码。由于您尚未为 `root` 用户分配密码，所以只需按 **Enter** 键。

:::

以下 curl 命令虽然看起来复杂，但在本教程最后部分提供了详细解释。当前，建议您先运行该命令并导入数据，然后在教程结束后了解有关数据导入的详细内容。

### 导入纽约市交通事故数据

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

以下是上述命令的输出。第一个突出显示的部分显示了您应看到的内容（OK 和除一行之外的所有行都已插入）。由于其中一行包含不正确数据，该行在导入过程中被过滤。

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

如果发生错误，StarRocks 会返回一个 URL 来帮助您查看错误消息。由于容器具有私有 IP 地址，您必须通过从容器运行 curl 来查看。

```bash
curl http://10.5.0.3:8040/api/_load_error_log<details from ErrorURL>
```

点击展开查看 URL 中包含的错误内容：

<details>

<summary>错误信息</summary>

```bash
Error: Value count does not match column count. Expect 29, but got 32.
Column delimiter: 44,Row delimiter: 10.. Row: 09/06/2015,14:15,,,40.6722269,-74.0110059,"(40.6722269, -74.0110059)",,,"R/O 1 BEARD ST. ( IKEA'S 
09/14/2015,5:30,BRONX,10473,40.814551,-73.8490955,"(40.814551, -73.8490955)",TORRY AVENUE                    ,NORTON AVENUE                   ,,0,0,0,0,0,0,0,0,Driver Inattention/Distraction,Unspecified,,,,3297457,PASSENGER VEHICLE,PASSENGER VEHICLE,,,
```

</details>

### 导入天气数据

按照上述方式导入天气数据：

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
在导入数据时，`starrocks/shared/` 下的文件夹名称是动态生成的。您会在 `shared` 下面看到一个路径，然后在该路径下看到另外两个路径。在每个路径内，您将找到 `data`、`metadata` 和 `schema` 路径。

![MinIO Web UI](../assets/quick-start/MinIO-data.png)
:::

---

## 查询数据并回答问题

<SQL />

---

## 存算分离集群配置细节

现在您已经体验了使用 StarRocks 存算分离集群，所以理解其配置是非常重要的。

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

FE 配置与默认配置略有不同，因为必须配置 FE 以使其将数据存储在对象存储中，而不是 BE 节点的本地磁盘上。

`docker-compose.yml` 文件在 `command` 中生成 FE 配置。

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

以上命令生成了以下配置文件：

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

:::note
此配置文件包含默认部署参数以及存算分离集群部署的附加参数，其中存算分离集群部署的参数已经突出显示。
:::

这些参数的具体解释如下：

:::note
以下许多配置参数都以 `aws_s3_` 为前缀。该前缀用于所有兼容 Amazon S3 存储类型（例如：S3、GCS 和 MinIO）。在使用 Azure Blob Storage 时，前缀是 `azure_`。
:::

#### `run_mode=shared_data`

该参数用于启用存算分离。

#### `aws_s3_path=starrocks`

存储桶名称。

#### `aws_s3_endpoint=minio:9000`

MinIO 端点，包括端口号。

#### `aws_s3_use_instance_profile=false`

在使用 MinIO 时，使用访问密钥，因此不使用 Instance Profile 文件。

#### `cloud_native_storage_type=S3`

指定使用 S3 兼容存储或者 Azure Blob 存储。对于 MinIO，该项需要设置为 S3。

#### `aws_s3_use_aws_sdk_default_behavior=false`

在使用 MinIO 时，此参数始终设置为 false。

---

## 总结

在本教程中，您完成了以下目标：

- 在 Docker 中部署了 StarRocks 和 MinIO
- 创建了 MinIO 访问密钥
- 配置了使用 MinIO 的 StarRocks Storage Volume
- 向 StarRocks 导入了由纽约市提供的交通事故数据和由 NOAA 提供的天气数据
- 使用 SQL JOIN 分析了数据，并得出以下结论：能见度低或路面结冰时，驾驶机动车较平时更加危险。

除以上内容外，本教程有意地略过了在 Stream Load 过程中进行的数据转换。关于此内容的详细信息在下一小节中提供。

## 关于 curl 命令的注释

<Curl />

## 更多信息

[StarRocks 表设计](../table_design/StarRocks_table_design.md)

[物化视图](../cover_pages/mv_use_cases.mdx)

[Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)

[纽约市交通事故数据](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) 数据集由纽约市提供，受到这些[使用条款](https://www.nyc.gov/home/terms-of-use.page)和[隐私政策](https://www.nyc.gov/home/privacy-policy.page)的约束。

[气象数据](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD)由 NOAA 提供，附带此[免责声明](https://www.noaa.gov/disclaimer)和此[隐私政策](https://www.noaa.gov/protecting-your-privacy)。