// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.statistic.StatisticUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class IcebergPartitionUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);

    // Normalize partition name to yyyy-MM-dd (Type is Date) or yyyy-MM-dd HH:mm:ss (Type is Datetime)
    // Iceberg partition field transform support year, month, day, hour now,
    // eg.
    // year(ts)  partitionName : 2023              return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // month(ts) partitionName : 2023-01           return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // day(ts)   partitionName : 2023-01-01        return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // hour(ts)  partitionName : 2023-01-01-12     return 2023-01-01 12:00:00 (Datetime)
    public static String normalizeTimePartitionName(String partitionName, PartitionField partitionField, Schema schema,
                                                    Type type) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        boolean parseFromDate = true;
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == IcebergPartitionTransform.YEAR) {
            partitionName += "-01-01";
        } else if (transform == IcebergPartitionTransform.MONTH) {
            partitionName += "-01";
        } else if (transform == IcebergPartitionTransform.DAY) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else if (transform == IcebergPartitionTransform.HOUR) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
            parseFromDate = false;
        } else {
            throw new StarRocksConnectorException("Unsupported partition transform to normalize: %s",
                    partitionField.transform().toString());
        }

        // partition name formatter
        DateTimeFormatter formatter = null;
        if (type.isDate()) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        }
        // If has timestamp with time zone, should compute the time zone offset to UTC
        ZoneId zoneId;
        if (schema.findType(partitionField.sourceId()).equals(Types.TimestampType.withZone())) {
            zoneId = TimeUtils.getTimeZone().toZoneId();
        } else {
            zoneId = ZoneOffset.UTC;
        }

        String result;
        try {
            if (parseFromDate) {
                LocalDate date = LocalDate.parse(partitionName, dateTimeFormatter);
                if (type.isDate()) {
                    result = date.format(formatter);
                } else {
                    LocalDateTime dateTime = date.atStartOfDay().atZone(ZoneOffset.UTC).
                            withZoneSameInstant(zoneId).toLocalDateTime();
                    result = dateTime.format(formatter);
                }
            } else {
                // parse from datetime which contains hour
                LocalDateTime dateTime = LocalDateTime.parse(partitionName, dateTimeFormatter).atZone(ZoneOffset.UTC).
                        withZoneSameInstant(zoneId).toLocalDateTime();
                result = dateTime.format(formatter);
            }
        } catch (Exception e) {
            LOG.warn("parse partition name failed, partitionName: {}, partitionField: {}, type: {}",
                    partitionName, partitionField, type);
            throw new StarRocksConnectorException("parse/format partition name failed", e);
        }
        return result;
    }

    // Get the date interval from iceberg partition transform
    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromIceberg(IcebergTable table,
                                                                                Column partitionColumn) {
        PartitionField partitionField = table.getPartitionFiled(partitionColumn.getName());
        if (partitionField == null) {
            throw new StarRocksConnectorException("Partition column %s not found in table %s.%s.%s",
                    partitionColumn.getName(), table.getCatalogName(), table.getRemoteDbName(), table.getRemoteTableName());
        }
        String transform = partitionField.transform().toString();
        IcebergPartitionTransform icebergPartitionTransform = IcebergPartitionTransform.fromString(transform);
        switch (icebergPartitionTransform) {
            case YEAR:
                return PartitionUtil.DateTimeInterval.YEAR;
            case MONTH:
                return PartitionUtil.DateTimeInterval.MONTH;
            case DAY:
                return PartitionUtil.DateTimeInterval.DAY;
            case HOUR:
                return PartitionUtil.DateTimeInterval.HOUR;
            default:
                return PartitionUtil.DateTimeInterval.NONE;
        }
    }

    public static boolean isSupportedConvertPartitionTransform(IcebergPartitionTransform transform) {
        return transform == IcebergPartitionTransform.IDENTITY ||
                transform == IcebergPartitionTransform.YEAR ||
                transform == IcebergPartitionTransform.MONTH ||
                transform == IcebergPartitionTransform.DAY ||
                transform == IcebergPartitionTransform.HOUR;
    }

    public static LocalDateTime addDateTimeInterval(LocalDateTime dateTime, IcebergPartitionTransform transform) {
        switch (transform) {
            case YEAR:
                return dateTime.plusYears(1);
            case MONTH:
                return dateTime.plusMonths(1);
            case DAY:
                return dateTime.plusDays(1);
            case HOUR:
                return dateTime.plusHours(1);
            default:
                throw new StarRocksConnectorException("Unsupported partition transform to add: %s", transform);
        }
    }

    /**
        convert partition value to predicate
        eg.
        partitionColumn: ts(date)
        partitionValue: 2023  transform: year
        return ts >= '2023-01-01' and ts < '2024-01-01'
        partitionValue: 2023-01 transform: month
        return ts >= '2023-01-01' and ts < '2023-02-01'
        partitionValue: 2023-01-01  transform: day
        return ts >= '2023-01-01' and ts < '2023-01-02'

        partitionColumn: ts(datetime)   transform: year
        partitionValue: 2023  transform: year
        return ts >= '2023-01-01 00:00:00' and ts < '2024-01-01 00:00:00'
        partitionValue: 2023-01 transform: month
        return ts >= '2023-01-01 00:00:00' and ts < '2023-02-01 00:00:00'
        partitionValue: 2023-01-01  transform: day
        return ts >= '2023-01-01 00:00:00' and ts < '2023-01-02 00:00:00'
        partitionValue: 2023-01-01-12  transform: hour
        return ts >= '2023-01-01 12:00:00' and ts < '2023-01-01 13:00:00'
    */
    public static String convertPartitionFieldToPredicate(IcebergTable table, String partitionColumn,
                                                          String partitionValue) {
        PartitionField partitionField = table.getPartitionFiled(partitionColumn);
        if (partitionField == null) {
            throw new StarRocksConnectorException("Partition column %s not found in table %s.%s.%s",
                    partitionColumn, table.getCatalogName(), table.getRemoteDbName(), table.getRemoteTableName());
        }
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == IcebergPartitionTransform.IDENTITY) {
            return StatisticUtils.quoting(partitionColumn) + " = '" + partitionValue + "'";
        } else {
            // transform is year, month, day, hour
            Type partitiopnColumnType = table.getColumn(partitionColumn).getType();
            Preconditions.checkState(partitiopnColumnType.isDateType(),
                    "Partition column %s type must be date or datetime", partitionColumn);
            String normalizedPartitionValue = normalizeTimePartitionName(partitionValue, partitionField,
                    table.getNativeTable().schema(), partitiopnColumnType);

            LocalDateTime startDateTime = null;
            DateTimeFormatter dateTimeFormatter = null;
            if (partitiopnColumnType.isDate()) {
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                startDateTime = LocalDate.parse(normalizedPartitionValue, dateTimeFormatter).atStartOfDay();
            } else {
                dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                startDateTime = LocalDateTime.parse(normalizedPartitionValue, dateTimeFormatter);
            }
            LocalDateTime endDateTime = addDateTimeInterval(startDateTime, transform);
            String endDateTimeStr = endDateTime.format(dateTimeFormatter);

            return StatisticUtils.quoting(partitionColumn) + " >= '" + normalizedPartitionValue + "' and " +
                    StatisticUtils.quoting(partitionColumn) + " < '" + endDateTimeStr + "'";
        }
    }
}