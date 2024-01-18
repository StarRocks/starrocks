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


package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Maps;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.structure.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class CsvFileStatisticsStorage implements StatisticStorage {
    private static final Logger LOG = LogManager.getLogger(CsvFileStatisticsStorage.class);

    private final Map<Pair<String, String>, StatisticsEntry> columnStatisticMap = Maps.newHashMap();

    private String path;

    public void setPath(String path) {
        this.path = path;
    }

    public CsvFileStatisticsStorage(String path) {
        this.path = path;
    }

    public void parse() {
        if (path == null || path.isEmpty()) {
            LOG.warn("path is empty");
            return;
        }

        Path myPath = Paths.get(path);
        CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
        try {
            BufferedReader
                    br = Files.newBufferedReader(myPath,
                    StandardCharsets.UTF_8);
            CSVReader reader = new CSVReaderBuilder(br).withCSVParser(parser).build();
            String[] nextLine;

            while ((nextLine = reader.readNext()) != null) {
                StatisticsEntry entry = new StatisticsEntry();
                entry.setValue(nextLine);
                columnStatisticMap.put(new Pair<>(entry.tableName.split("\\.")[1], entry.columnName), entry);
            }
        } catch (Exception e) {
            LOG.warn("parse csv file failed, " + e);
        }
    }

    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columnNames) {
        return columnNames.stream().map(columnName -> getColumnStatistic(table, columnName))
                .collect(Collectors.toList());
    }

    @Override
    public ColumnStatistic getColumnStatistic(Table table, String columnName) {
        StatisticsEntry statisticsEntry = columnStatisticMap.get(new Pair<>(table.getName(), columnName));
        try {
            return convert2ColumnStatistics(table, statisticsEntry);
        } catch (Exception e) {
            LOG.warn("convert to column statistics failed");
        }
        return null;
    }

    private ColumnStatistic convert2ColumnStatistics(Table table, StatisticsEntry statisticsEntry)
            throws AnalysisException {
        Column column = table.getColumn(statisticsEntry.columnName);
        if (column == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR, statisticsEntry.columnName);
        }

        ColumnStatistic.Builder builder = ColumnStatistic.builder();
        double minValue = Double.NEGATIVE_INFINITY;
        double maxValue = Double.POSITIVE_INFINITY;
        try {
            if (column.getPrimitiveType().equals(PrimitiveType.DATE)) {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                if (statisticsEntry.isSetMin() && !statisticsEntry.min.isEmpty()) {
                    minValue = getLongFromDateTime(LocalDate.parse(statisticsEntry.min, dtf).atStartOfDay());
                }
                if (statisticsEntry.isSetMax() && !statisticsEntry.max.isEmpty()) {
                    maxValue = getLongFromDateTime(LocalDate.parse(statisticsEntry.max, dtf).atStartOfDay());
                }
            } else if (column.getPrimitiveType().equals(PrimitiveType.DATETIME)) {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                if (statisticsEntry.isSetMin() && !statisticsEntry.min.isEmpty()) {
                    minValue = getLongFromDateTime(LocalDateTime.parse(statisticsEntry.min, dtf));
                }
                if (statisticsEntry.isSetMax() && !statisticsEntry.max.isEmpty()) {
                    maxValue = getLongFromDateTime(LocalDateTime.parse(statisticsEntry.max, dtf));
                }
            } else {
                try {
                    if (statisticsEntry.isSetMin() && !statisticsEntry.min.isEmpty()) {
                        minValue = Double.parseDouble(statisticsEntry.min);
                    }
                } catch (Exception m) {
                    minValue = Double.MIN_VALUE;
                }

                try {
                    if (statisticsEntry.isSetMax() && !statisticsEntry.max.isEmpty()) {
                        maxValue = Double.parseDouble(statisticsEntry.max);
                    }
                } catch (Exception e) {
                    maxValue = Double.MAX_VALUE;
                }
            }
        } catch (Exception e) {
            LOG.warn("convert TStatisticData to ColumnStatistics failed, table : {}, column : {}, errMsg : {}",
                    table.getName(), column.getName(), e.getMessage());
        }

        return builder.setMinValue(minValue).
                setMaxValue(maxValue).
                setDistinctValuesCount(Double.parseDouble(statisticsEntry.distinctCount)).
                setAverageRowSize(Double.parseDouble(statisticsEntry.dataSize) /
                        Math.max(Double.parseDouble(statisticsEntry.rowCount), 1)).
                setNullsFraction(Double.parseDouble(statisticsEntry.nullCount) /
                        Math.max(Double.parseDouble(statisticsEntry.rowCount), 1)).build();
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
    }

    private static class StatisticsEntry {
        public String tableId;
        public String columnName;
        public String dbId;
        public String tableName;
        public String dbName;
        public String rowCount;
        public String dataSize;
        public String distinctCount;
        public String nullCount;
        public String max;
        public String min;
        public String updateTime;

        public void setValue(String[] row) {
            if (row.length != 12) {
                LOG.warn("row miss some field");
                return;
            }
            this.tableId = row[0];
            this.columnName = row[1];
            this.dbId = row[2];
            this.tableName = row[3];
            this.dbName = row[4];
            this.rowCount = row[5];
            this.dataSize = row[6];
            this.distinctCount = row[7];
            this.nullCount = row[8];
            this.max = row[9];
            this.min = row[10];
            this.updateTime = row[11];
        }

        public boolean isSetMin() {
            return this.min != null;
        }

        public boolean isSetMax() {
            return this.max != null;
        }
    }
}
