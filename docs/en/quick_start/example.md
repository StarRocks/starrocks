---
displayed_sidebar: "English"
sidebar_position: 1
---

# Learn by doing

This tutorial covers:

- Running StarRocks in a single Docker container
- Loading two public datasets including basic transformation of the data
- Analyzing the data with SELECT and JOIN

The data used is provided by NYC OpenData and the National Centers for Environmental Information.

Both of these datasets are very large, and because this tutorial is intended to help you get exposed to working with StarRocks we are not going to load data for the past 120 years. You can run the Docker image and load this data on a machine with 4GB RAM assigned to Docker. For larger fault-tolerant and scalable deployments we have other documentation and will provide that later.

## Prerequisites

### Docker

### Disk space

### cURL


## Launch StarRocks

```bash
sudo docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 \
    -itd starrocks/allin1-ubuntu
```

## Download the data

Download these two datasets to your machine. You can download them to the host machine where you are running Docker, they do not need to be downloaded inside the container.

### New York City crash data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/starrocks/b68318323c544552900ec3ad5517e6ad4a1175d5/docs/en/quick_start/_data/NYPD_Crash_Data.csv
```

### Weather data

```bash
curl -O https://raw.githubusercontent.com/StarRocks/starrocks/b68318323c544552900ec3ad5517e6ad4a1175d5/docs/en/quick_start/_data/72505394728.csv
```

## Connect to StarRocks

You can use most SQL clients that work with MySQL. This tutorial is tested with the `mysql` client and with DBeaver as these are the two most commonly used clients among the StarRocks community.

If you have the `mysql` client installed you can use that directly, if you need a client then install either `mysql` or DBeaver.

NOTE: Add details on installing both clients here

```sql
mysql -P9030 -h127.0.0.1 -uroot --prompt="StarRocks > "
```

### Create a database

Type these two lines in at the `StarRocks > ` prompt and press enter after each:

```sql
CREATE DATABASE IF NOT EXISTS quickstart;

USE quickstart;
```

### Create two tables

#### Crashdata

```sql
CREATE TABLE IF NOT EXISTS crashdata (
    CRASH_DATE DATETIME,
    BOROUGH STRING,
    ZIP_CODE STRING,
    LATITUDE INT,
    LONGITUDE INT,
    LOCATION STRING,
    ON_STREET_NAME STRING,
    CROSS_STREET_NAME STRING,
    OFF_STREET_NAME STRING,
    CONTRIBUTING_FACTOR_VEHICLE_1 STRING,
    CONTRIBUTING_FACTOR_VEHICLE_2 STRING,
    COLLISION_ID INT,
    VEHICLE_TYPE_CODE_1 STRING,
    VEHICLE_TYPE_CODE_2 STRING
);
```

#### Weatherdata

```sql
CREATE TABLE IF NOT EXISTS weatherdata (
    DATE DATETIME,
    NAME STRING,
    HourlyDewPointTemperature STRING,
    HourlyDryBulbTemperature STRING,
    HourlyPrecipitation STRING,
    HourlyPresentWeatherType STRING,
    HourlyPressureChange STRING,
    HourlyPressureTendency STRING,
    HourlyRelativeHumidity STRING,
    HourlySkyConditions STRING,
    HourlyVisibility STRING,
    HourlyWetBulbTemperature STRING,
    HourlyWindDirection STRING,
    HourlyWindGustSpeed STRING,
    HourlyWindSpeed STRING
);
```

### Load the NYC Crash data for 2014 and 2015

Generally you will load data using a tool like ?????. Since this is a tutorial to get started with StarRocks we are
using cURL and the built-in stream load mechanism. Stream load and cURL are popular when loading files from the local filesystem. 

:::tip
Open a new shell as these cURL commands are run at the operating system prompt, not in the `mysql` client. The commands refer to the datasets that you downloaded, so run them from the directory where you downloaded the files.

You will be prompted for a password. You probably have not assigned a password to the MySQL `root` user, so just hit enter.
:::

Run the command and then read about the options.

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


:::tip
Notes on the above command:

- `--location-trusted`
- `-u root`
- `-T filename`
- `label:name-num`
- `column_separator:,`
- `skip_header:1`
- `enclose:\"`
- `max_filter_ratio:1`
- `columns:`
:::

### Load the weather data for 2014 and 2015

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

## Answer some questions

These queries can be run in your SQL client.

#### How many crashes are there per hour in NYC?

```sql
SELECT COUNT(*),
       date_trunc("hour", crashdata.CRASH_DATE) AS Time
FROM crashdata
GROUP BY Time
ORDER BY Time ASC
LIMIT 200;
```

Here is part of the output. Note that I am looking closer at January 6th and 7th as this is Monday and Tuesday of a non-holiday week. Looking at New Years Day is probably not indicative of a normal morning during rush-hour traffic.

```plaintext
|       14 | 2014-01-06 06:00:00 |
|       16 | 2014-01-06 07:00:00 |
# highlight-start
|       43 | 2014-01-06 08:00:00 |
|       44 | 2014-01-06 09:00:00 |
|       21 | 2014-01-06 10:00:00 |
# highlight-end
|       28 | 2014-01-06 11:00:00 |
|       34 | 2014-01-06 12:00:00 |
|       31 | 2014-01-06 13:00:00 |
|       35 | 2014-01-06 14:00:00 |
|       36 | 2014-01-06 15:00:00 |
|       33 | 2014-01-06 16:00:00 |
|       40 | 2014-01-06 17:00:00 |
|       35 | 2014-01-06 18:00:00 |
|       23 | 2014-01-06 19:00:00 |
|       16 | 2014-01-06 20:00:00 |
|       12 | 2014-01-06 21:00:00 |
|       17 | 2014-01-06 22:00:00 |
|       14 | 2014-01-06 23:00:00 |
|       10 | 2014-01-07 00:00:00 |
|        4 | 2014-01-07 01:00:00 |
|        1 | 2014-01-07 02:00:00 |
|        3 | 2014-01-07 03:00:00 |
|        2 | 2014-01-07 04:00:00 |
|        6 | 2014-01-07 06:00:00 |
|       16 | 2014-01-07 07:00:00 |
# highlight-start
|       41 | 2014-01-07 08:00:00 |
|       37 | 2014-01-07 09:00:00 |
|       33 | 2014-01-07 10:00:00 |
# highlight-end
```

It looks like about 40 accidents on a Monday or Tuesday morning during rush hour traffic, and around the same at 17:00 hours.

#### What is the average temperature in NYC?

```sql
SELECT avg(HourlyDryBulbTemperature),
       date_trunc("hour", weatherdata.DATE) AS Time
FROM weatherdata
GROUP BY Time
ORDER BY Time ASC
LIMIT 100;
```

Output:

Note that this is data from 2014, NYC has not been this cold lately.

```plaintext
+-------------------------------+---------------------+
| avg(HourlyDryBulbTemperature) | Time                |
+-------------------------------+---------------------+
|                            25 | 2014-01-01 00:00:00 |
|                            25 | 2014-01-01 01:00:00 |
|                            24 | 2014-01-01 02:00:00 |
|                            24 | 2014-01-01 03:00:00 |
|                            24 | 2014-01-01 04:00:00 |
|                            24 | 2014-01-01 05:00:00 |
|                            25 | 2014-01-01 06:00:00 |
|                            26 | 2014-01-01 07:00:00 |
```

#### Is it safe to drive in NYC when visibility is poor?

Let's look at the number of crashes when visibility is poor (between 0 and 1.0 miles). To answer this question use a JOIN across the two tables on the DATETIME column.

```sql
SELECT COUNT(DISTINCT c.COLLISION_ID) AS Crashes,
       truncate(avg(w.HourlyDryBulbTemperature), 1) AS Temp_F,
       truncate(avg(w.HourlyVisibility), 2) AS Visibility,
       max(w.HourlyPrecipitation) AS Precipitation,
       date_format((date_trunc("hour", c.CRASH_DATE)), '%d %b %Y %H:%i') AS Hour
FROM crashdata c
LEFT JOIN weatherdata w
ON date_trunc("hour", c.CRASH_DATE)=date_trunc("hour", w.DATE)
WHERE w.HourlyVisibility BETWEEN 0.0 AND 1.0
GROUP BY Hour
ORDER BY Crashes DESC
LIMIT 100;
```

The highest number of crashes in a single hour during low visibility is 129. There are multiple things to consider:

- February 3rd 2014 was a Monday
- 8AM is rush hour
- It was raining (0.12 inches or precipitation that hour)
- The temperature is 32 degrees F (the freezing point for water)
- Visibility is bad at 0.25 miles, normal for NYC is 10 miles

```plaintext
+---------+--------+------------+---------------+-------------------+
| Crashes | Temp_F | Visibility | Precipitation | Hour              |
+---------+--------+------------+---------------+-------------------+
|     129 |     32 |       0.25 | 0.12          | 03 Feb 2014 08:00 |
|     114 |     32 |       0.25 | 0.12          | 03 Feb 2014 09:00 |
|     104 |     23 |       0.33 | 0.03          | 09 Jan 2015 08:00 |
|      96 |   26.3 |       0.33 | 0.07          | 01 Mar 2015 14:00 |
|      95 |     26 |       0.37 | 0.12          | 01 Mar 2015 15:00 |
|      93 |     35 |       0.75 | 0.09          | 18 Jan 2015 09:00 |
|      92 |     31 |       0.25 | 0.12          | 03 Feb 2014 10:00 |
|      87 |   26.8 |        0.5 | 0.09          | 01 Mar 2015 16:00 |
|      85 |     55 |       0.75 | 0.20          | 23 Dec 2015 17:00 |
|      85 |     20 |       0.62 | 0.01          | 06 Jan 2015 11:00 |
|      83 |   19.6 |       0.41 | 0.04          | 05 Mar 2015 13:00 |
|      80 |     20 |       0.37 | 0.02          | 06 Jan 2015 10:00 |
|      76 |   26.5 |       0.25 | 0.06          | 05 Mar 2015 09:00 |
|      71 |     26 |       0.25 | 0.09          | 05 Mar 2015 10:00 |
|      71 |   24.2 |       0.25 | 0.04          | 05 Mar 2015 11:00 |
```

#### What about driving in icy conditions?

Water vapor can desublimate to ice at 40 degrees F; this query looks at temps between 0 and 40 degrees F.

```sql
SELECT COUNT(DISTINCT c.COLLISION_ID) AS Crashes,
       truncate(avg(w.HourlyDryBulbTemperature), 1) AS Temp_F,
       truncate(avg(w.HourlyVisibility), 2) AS Visibility,
       max(w.HourlyPrecipitation) AS Precipitation,
       date_format((date_trunc("hour", c.CRASH_DATE)), '%d %b %Y %H:%i') AS Hour
FROM crashdata c
LEFT JOIN weatherdata w
ON date_trunc("hour", c.CRASH_DATE)=date_trunc("hour", w.DATE)
WHERE w.HourlyDryBulbTemperature BETWEEN 0.0 AND 40.5 
GROUP BY Hour
ORDER BY Crashes DESC
LIMIT 100;
```

The results for freezing temperatures suprised me a little, I did not expect too much traffic on a Sunday morning in the city on a cold January day.A quick look at [weather.com](https://weather.com/storms/winter/news/northeast-storm-rain-snow-wind) showed that there was a big storm with many crashes that day, just like what can be seen in the data.

```plaintext
+---------+--------+------------+---------------+-------------------+
| Crashes | Temp_F | Visibility | Precipitation | Hour              |
+---------+--------+------------+---------------+-------------------+
|     192 |     34 |        1.5 | 0.09          | 18 Jan 2015 08:00 |
|     170 |     21 |       NULL |               | 21 Jan 2014 10:00 |
|     145 |     19 |       NULL |               | 21 Jan 2014 11:00 |
|     138 |   33.5 |          5 | 0.02          | 18 Jan 2015 07:00 |
|     137 |     21 |       NULL |               | 21 Jan 2014 09:00 |
|     129 |     32 |       0.25 | 0.12          | 03 Feb 2014 08:00 |
|     114 |     32 |       0.25 | 0.12          | 03 Feb 2014 09:00 |
|     104 |     23 |        0.7 | 0.04          | 09 Jan 2015 08:00 |
|      98 |     16 |          8 | 0.00          | 06 Mar 2015 08:00 |
|      96 |   26.3 |       0.33 | 0.07          | 01 Mar 2015 14:00 |
```

Drive carefully!

## More information

Link to NYC Open Data

Link to NOAA

Details for stream load

NOTE: Make sure to rewrite the loading/StreamLoad doc

