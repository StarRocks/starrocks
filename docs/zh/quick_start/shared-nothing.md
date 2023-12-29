---
displayed_sidebar: "Chinese"
sidebar_position: 1
description: "存算一体"
---
import DDL from '../assets/quick-start/_DDL.mdx'
import Clients from '../assets/quick-start/_clientsAllin1.mdx'
import SQL from '../assets/quick-start/_SQL.mdx'
import Curl from '../assets/quick-start/_curl.mdx'

# StarRocks 基础知识

本教程涵盖了以下内容：

- 在单个 Docker 容器中运行 StarRocks
- 导入两个公共数据集
- 使用 SELECT 和 JOIN 分析数据
- 基本数据转换（ETL 中的 **T**）

以下使用的数据由 NYC OpenData 和 NOAA 的 National Centers for Environmental Information 提供。

由于这两个数据集都非常庞大，而本教程旨在帮助您熟悉使用 StarRocks，因此只会使用数据集中的部分数据。您可以在分配了 4GB RAM 的机器上运行 Docker 镜像并导入这些数据。关于高可用和可扩展的部署，请参考部署章节。

本文以操作步骤开始，旨在实现以下目的：

1. 指导读者导入数据并分析该数据。
2. 解释导入期间数据转换的基础知识。

---

## Prerequisites

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

FE 节点负责元数据管理、客户端连接管理、查询计划和查询调度。每个 FE 在其内存中存储和维护完整的元数据副本，这确保了每个 FE 都能提供无差别的服务。

### BE

BE 节点在**存算一体**集群中负责数据存储和执行查询。

---

## 启动 StarRocks

```bash
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd \
--name quickstart starrocks/allin1-ubuntu
```
---
## SQL 客户端

<Clients />

---

## 下载数据

将两个数据集下载到您的主机上。您可以将它们直接下载到运行 Docker 的主机机器上，无需在容器内下载。

### 下载纽约市交通事故数据

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

### 下载天气数据

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

### 使用 SQL 客户端连接到 StarRocks

:::tip

如果您使用的是 mysql CLI 以外的客户端，请打开该客户端。
:::

以下命令将在 Docker 容器中运行 `mysql` 命令：

```sql
docker exec -it quickstart \
mysql -P 9030 -h 127.0.0.1 -u root --prompt="StarRocks > "
```

---

## 建表

<DDL />

---

## 导入数据集
StarRocks 有多种数据导入方法。对于本教程，最简单的方法是使用 curl 命令发起 Stream Load 任务导入数据。

:::tip
请打开一个新的 shell 终端。以下 curl 命令在需要在操作系统的终端中运行，而不是在 mysql 客户端中运行。同时，因为这些命令引用了您下载的数据集，因此您需要从保存数据文件的路径运行以下命令。

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
    "ErrorURL": "http://127.0.0.1:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f"
    # highlight-end
}%
```

如果发生错误，StarRocks 会返回一个 URL 来帮助您查看错误消息。由于容器具有私有 IP 地址，您开源在浏览器中打开此 URL 查看。点击展开查看 URL 中包含的错误内容：

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

## 查询数据并回答问题

<SQL />

---

## 总结

在本教程中，您完成了以下目标：

- 在 Docker 中部署了 StarRocks
- 向 StarRocks 导入了由纽约市提供的交通事故数据和由 NOAA 提供的天气数据
- 使用 SQL JOIN 分析了数据，并得出以下结论：能见度低或路面结冰时，驾驶机动车较平时更加危险。

除以上内容外，本教程有意地略过了在 Stream Load 过程中进行的数据转换。关于此内容的详细信息在下一小节中提供。

---

## 关于 curl 命令的注释

<Curl />

---

## 更多信息

[StarRocks 表设计](../table_design/StarRocks_table_design.md)

[物化视图](../cover_pages/mv_use_cases.mdx)

[Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)

[纽约市交通事故数据](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) 数据集由纽约市提供，受到以下[使用条款](https://www.nyc.gov/home/terms-of-use.page)和[隐私政策](https://www.nyc.gov/home/privacy-policy.page)的约束。

[气象数据](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD)由 NOAA 提供，附带以下[免责声明](https://www.noaa.gov/disclaimer)和[隐私政策](https://www.noaa.gov/protecting-your-privacy)。
