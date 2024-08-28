---
displayed_sidebar: docs
sidebar_position: 1
description: "存算一体"
---
import DDL from '../_assets/quick-start/_DDL.mdx'
import Clients from '../_assets/quick-start/_clientsAllin1.mdx'
import SQL from '../_assets/quick-start/_SQL.mdx'
import Curl from '../_assets/quick-start/_curl.mdx'

# 存算一体

本教程涵盖了以下内容：

- 使用 Docker Compose 部署 StarRocks 存算一体集群。
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
当前教程不包含 CN 节点，以上内容仅供您了解 BE 和 CN 之间的区别。
:::

---

## 启动 StarRocks

运行以下命令启动 StarRocks 存算一体集群。

```bash
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -itd \
--name quickstart starrocks/allin1-ubuntu
```

---

## 下载数据集

将教程所需的数据集下载到您的主机上，无需在容器内下载。

### 下载纽约市交通事故数据

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/NYPD_Crash_Data.csv
```

### 下载天气数据

```bash
curl -O https://raw.githubusercontent.com/StarRocks/demo/master/documentation-samples/quickstart/datasets/72505394728.csv
```

---

## SQL 客户端

<Clients />

---

### 使用 SQL 客户端连接到 StarRocks

客户端配置完成后，连接至 StarRocks。

---

## 建表

<DDL />

---

## 导入数据集

StarRocks 提供多种数据导入方法。本教程使用 curl 命令发起 Stream Load 任务导入数据。

Stream Load 使用的 curl 命令虽然看起来复杂，但本教程在最后部分提供了详细解释。建议您先运行该命令导入数据，然后在教程结束后了解有关数据导入的详细内容。

### 导入纽约市交通事故数据

打开一个新的 Shell 终端，**进入数据集文件所在的路径**，然后运行以下命令。

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
    "ErrorURL": "http://127.0.0.1:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f"
    # highlight-end
}%
```

当 Stream Load 发生错误时，StarRocks 会返回一个包含错误信息的 URL，您可以通过 curl 命令查看。


```bash
curl http://127.0.0.1:8040/api/_load_error_log?file=error_log_da41dd88276a7bfc_739087c94262ae9f
```

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

## 查询数据并回答问题

<SQL />

---

## 总结

在本教程中，您完成了以下目标：

- 在 Docker 容器中部署了 StarRocks
- 向 StarRocks 导入了纽约市的交通事故数据和天气数据
- 使用 JOIN 查询分析数据

除以上内容外，本教程有意略过了在 Stream Load 命令中数据转换相关的参数说明。关于此内容的详细信息在下一小节中提供。

---

## 关于 curl 命令的注释

<Curl />

---

## 更多信息

- [StarRocks 表设计](../table_design/StarRocks_table_design.md)
- [物化视图](../cover_pages/mv_use_cases.mdx)
- [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md)
- [纽约市交通事故数据](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) 数据集由纽约市提供，受到以下[使用条款](https://www.nyc.gov/home/terms-of-use.page)和[隐私政策](https://www.nyc.gov/home/privacy-policy.page)的约束。
- [气象数据](https://www.ncdc.noaa.gov/cdo-web/datatools/lcd)(LCD)由 NOAA 提供，附带以下[免责声明](https://www.noaa.gov/disclaimer)和[隐私政策](https://www.noaa.gov/protecting-your-privacy)。
