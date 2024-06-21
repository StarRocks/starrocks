
```bash
gcloud container --project stone-victor-400120 \
  clusters create operator-docs \
  --location=southamerica-west1-b \
  --machine-type e2-standard-4 --disk-size 80 --num-nodes 3
```

```bash
echo -n 'g()()dpa$$word' > ./password.txt
```

```bash
kubectl create secret generic starrocks-root-password  --from-file=password=./password.txt
```

```bash
NEED TO SEE IF THIS WORKS WITHPROXY keytool -genkeypair \
        -alias starrocks \
        -keypass starrocks \
        -keyalg RSA \
        -keysize 1024 \
        -validity 365 \
        -keystore starrocks.jks \
        -storepass starrocks
```

```bash
NEED TO SEE IF THIS WORKS WITHPROXY kubectl create secret generic keystore \
  --from-file=data=./starrocks.jks
```

```bash
helm repo add starrocks-community https://starrocks.github.io/starrocks-kubernetes-operator
```

```bash
helm repo update starrocks-community
```

```bash
helm search repo starrocks-community
```

```bash
PROBABLY REMOVE kubectl create namespace sr-operator
```

```bash
helm install starrocks-operator starrocks-community/operator \
  -f starrocks-operator-values.yaml
```

```bash
helm install sr-dev starrocks-community/starrocks -f \
  starrocks-cluster-values.yaml
```

```bash
kubectl get pods
```

```bash
kubectl get pv
```

```bash
kubectl get pods
```

```bash
NAME                                         READY   STATUS      RESTARTS   AGE
kube-starrocks-be-0                          1/1     Running     0          2m48s
kube-starrocks-fe-0                          1/1     Running     0          3m56s
kube-starrocks-fe-proxy-56f8998799-lljfq     1/1     Running     0          2m48s
kube-starrocks-initpwd-t9257                 0/1     Completed   4          3m56s
starrocks-operator-operator-54b84fff-vhn85   1/1     Running     0          35m
```

```bash
kubectl get services
```

```bash
NAME                            TYPE         CLUSTER-IP     EXTERNAL-IP    PORT(S)                                                       AGE
kube-starrocks-be-search        ClusterIP    None           <none>         9050/TCP                                                      2m54s
kube-starrocks-be-service       LoadBalancer 34.118.225.160 34.176.129.227 9060:31707/TCP,8040:30206/TCP,9050:31362/TCP,8060:31772/TCP   2m54s
kube-starrocks-fe-proxy-service LoadBalancer 34.118.225.13  34.176.58.10   8080:32382/TCP                                                2m54s
kube-starrocks-fe-search        ClusterIP    None           <none>         9030/TCP                                                      4m2s
# highlight-next-line
kube-starrocks-fe-service       LoadBalancer 34.118.227.211 34.176.122.114 8030:31455/TCP,9020:30675/TCP,9030:31075/TCP,9010:31244/TCP   4m2s
kubernetes                      ClusterIP    34.118.224.1   <none>         443/TCP
```

```bash
export FE_PROXY=34.176.58.10:8080
```

```bash
mysql -h 34.176.122.114 -P 9030 -u root -p
```

```SQL
ADMIN SET FRONTEND CONFIG ("default_replication_num" = "1");
```

```bash
CREATE DATABASE IF NOT EXISTS quickstart;

USE quickstart;
```

```bash
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

```bash
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

```bash
curl --location-trusted -u root             \
    -T ./NYPD_Crash_Data.csv                \
    -H "label:crashdata-0"                  \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns:tmp_CRASH_DATE, tmp_CRASH_TIME, CRASH_DATE=str_to_date(concat_ws(' ', tmp_CRASH_DATE, tmp_CRASH_TIME), '%m/%d/%Y %H:%i'),BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5" \
    -XPUT http://$FE_PROXY/api/quickstart/crashdata/_stream_load
```

NOTE NEED A WAY TO GET the errors from stream load

The output of the curl command will include a link for the error message. Replace the hostname with the IP address of the `kube-starrocks-be-service` service:

http://34.176.129.227:8040/api/_load_error_log?file=error_log_7845fa3e880c83e4_533b53cce3f92fa8

```bash
curl --location-trusted -u root             \
    -T ./72505394728.csv                    \
    -H "label:weather-0"                    \
    -H "column_separator:,"                 \
    -H "skip_header:1"                      \
    -H "enclose:\""                         \
    -H "max_filter_ratio:1"                 \
    -H "columns: STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, REPORT_TYPE, SOURCE, HourlyAltimeterSetting, HourlyDewPointTemperature, HourlyDryBulbTemperature, HourlyPrecipitation, HourlyPresentWeatherType, HourlyPressureChange, HourlyPressureTendency, HourlyRelativeHumidity, HourlySkyConditions, HourlySeaLevelPressure, HourlyStationPressure, HourlyVisibility, HourlyWetBulbTemperature, HourlyWindDirection, HourlyWindGustSpeed, HourlyWindSpeed, Sunrise, Sunset, DailyAverageDewPointTemperature, DailyAverageDryBulbTemperature, DailyAverageRelativeHumidity, DailyAverageSeaLevelPressure, DailyAverageStationPressure, DailyAverageWetBulbTemperature, DailyAverageWindSpeed, DailyCoolingDegreeDays, DailyDepartureFromNormalAverageTemperature, DailyHeatingDegreeDays, DailyMaximumDryBulbTemperature, DailyMinimumDryBulbTemperature, DailyPeakWindDirection, DailyPeakWindSpeed, DailyPrecipitation, DailySnowDepth, DailySnowfall, DailySustainedWindDirection, DailySustainedWindSpeed, DailyWeather, MonthlyAverageRH, MonthlyDaysWithGT001Precip, MonthlyDaysWithGT010Precip, MonthlyDaysWithGT32Temp, MonthlyDaysWithGT90Temp, MonthlyDaysWithLT0Temp, MonthlyDaysWithLT32Temp, MonthlyDepartureFromNormalAverageTemperature, MonthlyDepartureFromNormalCoolingDegreeDays, MonthlyDepartureFromNormalHeatingDegreeDays, MonthlyDepartureFromNormalMaximumTemperature, MonthlyDepartureFromNormalMinimumTemperature, MonthlyDepartureFromNormalPrecipitation, MonthlyDewpointTemperature, MonthlyGreatestPrecip, MonthlyGreatestPrecipDate, MonthlyGreatestSnowDepth, MonthlyGreatestSnowDepthDate, MonthlyGreatestSnowfall, MonthlyGreatestSnowfallDate, MonthlyMaxSeaLevelPressureValue, MonthlyMaxSeaLevelPressureValueDate, MonthlyMaxSeaLevelPressureValueTime, MonthlyMaximumTemperature, MonthlyMeanTemperature, MonthlyMinSeaLevelPressureValue, MonthlyMinSeaLevelPressureValueDate, MonthlyMinSeaLevelPressureValueTime, MonthlyMinimumTemperature, MonthlySeaLevelPressure, MonthlyStationPressure, MonthlyTotalLiquidPrecipitation, MonthlyTotalSnowfall, MonthlyWetBulb, AWND, CDSD, CLDD, DSNW, HDSD, HTDD, NormalsCoolingDegreeDay, NormalsHeatingDegreeDay, ShortDurationEndDate005, ShortDurationEndDate010, ShortDurationEndDate015, ShortDurationEndDate020, ShortDurationEndDate030, ShortDurationEndDate045, ShortDurationEndDate060, ShortDurationEndDate080, ShortDurationEndDate100, ShortDurationEndDate120, ShortDurationEndDate150, ShortDurationEndDate180, ShortDurationPrecipitationValue005, ShortDurationPrecipitationValue010, ShortDurationPrecipitationValue015, ShortDurationPrecipitationValue020, ShortDurationPrecipitationValue030, ShortDurationPrecipitationValue045, ShortDurationPrecipitationValue060, ShortDurationPrecipitationValue080, ShortDurationPrecipitationValue100, ShortDurationPrecipitationValue120, ShortDurationPrecipitationValue150, ShortDurationPrecipitationValue180, REM, BackupDirection, BackupDistance, BackupDistanceUnit, BackupElements, BackupElevation, BackupEquipment, BackupLatitude, BackupLongitude, BackupName, WindEquipmentChangeDate" \
    -XPUT http://$FE_PROXY/api/quickstart/weatherdata/_stream_load
```

```bash
```

```bash
```

```bash
```

```bash
```

```bash
```

```bash
```

