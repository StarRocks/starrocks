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

package com.starrocks.metric;

import com.google.common.base.Strings;
import com.starrocks.authorization.AccessDeniedException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Unified metrics manager for all external connector ETL operations.
 * Tracks write, delete, and compaction metrics across connector types (iceberg, hive, etc.).
 *
 * Metric naming: {connector_type}_{operation}_{metric}
 * Examples: iceberg_write_total, hive_write_rows, iceberg_compaction_duration_ms_total
 */
public class ConnectorMetricsMgr {

    // Connector type constants
    public static final String CONNECTOR_ICEBERG = "iceberg";
    public static final String CONNECTOR_HIVE = "hive";

    /**
     * Per-connector metrics holder. Each connector type gets an isolated set of metric maps.
     */
    private static class ConnectorMetrics {
        // Write metrics
        final ConcurrentHashMap<String, LongCounterMetric> writeTotal = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> writeDurationMs = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> writeRows = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> writeBytes = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> writeFiles = new ConcurrentHashMap<>();

        // Delete metrics
        final ConcurrentHashMap<String, LongCounterMetric> deleteTotal = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> deleteDurationMs = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> deleteRows = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> deleteBytes = new ConcurrentHashMap<>();

        // Update metrics (UPDATE/MERGE with file_type label: data, position_delete)
        final ConcurrentHashMap<String, LongCounterMetric> updateTotal = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> updateDurationMs = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> updateRows = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> updateBytes = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> updateFiles = new ConcurrentHashMap<>();

        // Compaction metrics
        final ConcurrentHashMap<String, LongCounterMetric> compactionTotal = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> compactionDurationMs = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> compactionInputFiles = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> compactionOutputFiles = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, LongCounterMetric> compactionRemovedDeleteFiles = new ConcurrentHashMap<>();
    }

    private static final ConcurrentHashMap<String, ConnectorMetrics> METRICS_BY_CONNECTOR = new ConcurrentHashMap<>();

    // Constants
    public static final String STATUS_SUCCESS = "success";
    public static final String STATUS_FAILED = "failed";
    public static final String REASON_NONE = "none";
    public static final String REASON_UNKNOWN = "unknown";
    public static final String REASON_TIMEOUT = "timeout";
    public static final String REASON_OOM = "oom";
    public static final String REASON_ACCESS_DENIED = "access_denied";

    public static final String WRITE_TYPE_INSERT = "insert";
    public static final String WRITE_TYPE_OVERWRITE = "overwrite";
    public static final String WRITE_TYPE_CTAS = "ctas";

    public static final String DELETE_TYPE_POSITION = "position";
    public static final String DELETE_TYPE_METADATA = "metadata";

    public static final String FILE_TYPE_DATA = "data";
    public static final String FILE_TYPE_POSITION_DELETE = "position_delete";

    public static final String COMPACTION_TYPE_MANUAL = "manual";
    public static final String COMPACTION_TYPE_AUTO = "auto";

    private static ConnectorMetrics getOrCreate(String connectorType) {
        return METRICS_BY_CONNECTOR.computeIfAbsent(connectorType, k -> new ConnectorMetrics());
    }

    // ======================= Write Metrics =======================

    public static void increaseWriteTotal(String connectorType, String status, String reason, String writeType) {
        String normalizedStatus = normalizeStatus(status);
        String normalizedReason = normalizeReason(reason);
        String normalizedWriteType = normalizeWriteType(writeType);

        ConnectorMetrics m = getOrCreate(connectorType);
        String key = normalizedStatus + "|" + normalizedReason + "|" + normalizedWriteType;
        m.writeTotal.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_write_total",
                    Metric.MetricUnit.REQUESTS,
                    "total " + connectorType + " write tasks by status, reason and write type");
            metric.addLabel(new MetricLabel("status", normalizedStatus));
            metric.addLabel(new MetricLabel("reason", normalizedReason));
            metric.addLabel(new MetricLabel("write_type", normalizedWriteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(1L);
    }

    public static void increaseWriteTotalSuccess(String connectorType, String writeType) {
        increaseWriteTotal(connectorType, STATUS_SUCCESS, REASON_NONE, writeType);
    }

    public static void increaseWriteTotalFail(String connectorType, Throwable throwable, String writeType) {
        increaseWriteTotal(connectorType, STATUS_FAILED, classifyFailReason(throwable), writeType);
    }

    public static void increaseWriteTotalFail(String connectorType, String errorMessage, String writeType) {
        increaseWriteTotal(connectorType, STATUS_FAILED, classifyFailReason(errorMessage), writeType);
    }

    public static void increaseWriteDurationMs(String connectorType, long durationMs, String writeType) {
        String normalizedWriteType = normalizeWriteType(writeType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.writeDurationMs.computeIfAbsent(normalizedWriteType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_write_duration_ms_total",
                    Metric.MetricUnit.MILLISECONDS,
                    "total duration in milliseconds of " + connectorType + " write tasks by write type");
            metric.addLabel(new MetricLabel("write_type", normalizedWriteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(durationMs);
    }

    public static void increaseWriteRows(String connectorType, long rows, String writeType) {
        String normalizedWriteType = normalizeWriteType(writeType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.writeRows.computeIfAbsent(normalizedWriteType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_write_rows",
                    Metric.MetricUnit.ROWS,
                    "total written rows of " + connectorType + " write tasks by write type");
            metric.addLabel(new MetricLabel("write_type", normalizedWriteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(rows);
    }

    public static void increaseWriteBytes(String connectorType, long bytes, String writeType) {
        String normalizedWriteType = normalizeWriteType(writeType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.writeBytes.computeIfAbsent(normalizedWriteType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_write_bytes",
                    Metric.MetricUnit.BYTES,
                    "total written bytes of " + connectorType + " write tasks by write type");
            metric.addLabel(new MetricLabel("write_type", normalizedWriteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(bytes);
    }

    public static void increaseWriteFiles(String connectorType, long files, String writeType) {
        String normalizedWriteType = normalizeWriteType(writeType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.writeFiles.computeIfAbsent(normalizedWriteType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_write_files",
                    Metric.MetricUnit.NOUNIT,
                    "total number of data files written to " + connectorType + " by write type");
            metric.addLabel(new MetricLabel("write_type", normalizedWriteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(files);
    }

    // ======================= Delete Metrics =======================

    public static void increaseDeleteTotal(String connectorType, String status, String reason, String deleteType) {
        String normalizedStatus = normalizeStatus(status);
        String normalizedReason = normalizeReason(reason);
        String normalizedDeleteType = normalizeDeleteType(deleteType);

        ConnectorMetrics m = getOrCreate(connectorType);
        String key = normalizedStatus + "|" + normalizedReason + "|" + normalizedDeleteType;
        m.deleteTotal.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_delete_total",
                    Metric.MetricUnit.REQUESTS,
                    "total " + connectorType + " delete tasks by status, reason and delete type");
            metric.addLabel(new MetricLabel("status", normalizedStatus));
            metric.addLabel(new MetricLabel("reason", normalizedReason));
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(1L);
    }

    public static void increaseDeleteTotalSuccess(String connectorType, String deleteType) {
        increaseDeleteTotal(connectorType, STATUS_SUCCESS, REASON_NONE, deleteType);
    }

    public static void increaseDeleteTotalFail(String connectorType, Throwable throwable, String deleteType) {
        increaseDeleteTotal(connectorType, STATUS_FAILED, classifyFailReason(throwable), deleteType);
    }

    public static void increaseDeleteTotalFail(String connectorType, String errorMessage, String deleteType) {
        increaseDeleteTotal(connectorType, STATUS_FAILED, classifyFailReason(errorMessage), deleteType);
    }

    public static void increaseDeleteDurationMs(String connectorType, long durationMs, String deleteType) {
        String normalizedDeleteType = normalizeDeleteType(deleteType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.deleteDurationMs.computeIfAbsent(normalizedDeleteType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_delete_duration_ms_total",
                    Metric.MetricUnit.MILLISECONDS,
                    "total duration in milliseconds of " + connectorType + " delete tasks by delete type");
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(durationMs);
    }

    public static void increaseDeleteRows(String connectorType, long rows, String deleteType) {
        String normalizedDeleteType = normalizeDeleteType(deleteType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.deleteRows.computeIfAbsent(normalizedDeleteType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_delete_rows",
                    Metric.MetricUnit.ROWS,
                    "total deleted rows of " + connectorType + " delete tasks by delete type");
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(rows);
    }

    public static void increaseDeleteBytes(String connectorType, long bytes, String deleteType) {
        String normalizedDeleteType = normalizeDeleteType(deleteType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.deleteBytes.computeIfAbsent(normalizedDeleteType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_delete_bytes",
                    Metric.MetricUnit.BYTES,
                    "total deleted bytes of " + connectorType + " delete tasks by delete type");
            metric.addLabel(new MetricLabel("delete_type", normalizedDeleteType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(bytes);
    }

    // ======================= Update Metrics =======================

    public static void increaseUpdateTotal(String connectorType, String status, String reason) {
        String normalizedStatus = normalizeStatus(status);
        String normalizedReason = normalizeReason(reason);

        ConnectorMetrics m = getOrCreate(connectorType);
        String key = normalizedStatus + "|" + normalizedReason;
        m.updateTotal.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_update_total",
                    Metric.MetricUnit.REQUESTS,
                    "total " + connectorType + " update tasks by status and reason");
            metric.addLabel(new MetricLabel("status", normalizedStatus));
            metric.addLabel(new MetricLabel("reason", normalizedReason));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(1L);
    }

    public static void increaseUpdateTotalSuccess(String connectorType) {
        increaseUpdateTotal(connectorType, STATUS_SUCCESS, REASON_NONE);
    }

    public static void increaseUpdateTotalFail(String connectorType, Throwable throwable) {
        increaseUpdateTotal(connectorType, STATUS_FAILED, classifyFailReason(throwable));
    }

    public static void increaseUpdateTotalFail(String connectorType, String errorMessage) {
        increaseUpdateTotal(connectorType, STATUS_FAILED, classifyFailReason(errorMessage));
    }

    public static void increaseUpdateDurationMs(String connectorType, long durationMs) {
        ConnectorMetrics m = getOrCreate(connectorType);
        m.updateDurationMs.computeIfAbsent("default", k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_update_duration_ms_total",
                    Metric.MetricUnit.MILLISECONDS,
                    "total duration in milliseconds of " + connectorType + " update operations");
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(durationMs);
    }

    public static void increaseUpdateRows(String connectorType, long rows) {
        ConnectorMetrics m = getOrCreate(connectorType);
        m.updateRows.computeIfAbsent("default", k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_update_rows",
                    Metric.MetricUnit.ROWS,
                    "total rows affected by " + connectorType + " update operations");
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(rows);
    }

    public static void increaseUpdateBytes(String connectorType, long bytes, String fileType) {
        ConnectorMetrics m = getOrCreate(connectorType);
        m.updateBytes.computeIfAbsent(fileType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_update_bytes",
                    Metric.MetricUnit.BYTES,
                    "total bytes written by " + connectorType + " update operations by file type");
            metric.addLabel(new MetricLabel("file_type", fileType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(bytes);
    }

    public static void increaseUpdateFiles(String connectorType, long files, String fileType) {
        ConnectorMetrics m = getOrCreate(connectorType);
        m.updateFiles.computeIfAbsent(fileType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_update_files",
                    Metric.MetricUnit.NOUNIT,
                    "total files written by " + connectorType + " update operations by file type");
            metric.addLabel(new MetricLabel("file_type", fileType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(files);
    }

    // ======================= Compaction Metrics =======================

    public static void increaseCompactionTotal(String connectorType, String status, String reason,
                                               String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        String normalizedStatus = normalizeStatus(status);
        String normalizedReason = normalizeReason(reason);

        ConnectorMetrics m = getOrCreate(connectorType);
        String key = normalizedType + "|" + normalizedStatus + "|" + normalizedReason;
        m.compactionTotal.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_compaction_total",
                    Metric.MetricUnit.REQUESTS,
                    "total " + connectorType + " compaction tasks by type, status and reason");
            metric.addLabel(new MetricLabel("compaction_type", normalizedType));
            metric.addLabel(new MetricLabel("status", normalizedStatus));
            metric.addLabel(new MetricLabel("reason", normalizedReason));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(1L);
    }

    public static void increaseCompactionTotalSuccess(String connectorType, String compactionType) {
        increaseCompactionTotal(connectorType, STATUS_SUCCESS, REASON_NONE, compactionType);
    }

    public static void increaseCompactionTotalFail(String connectorType, Throwable throwable,
                                                   String compactionType) {
        increaseCompactionTotal(connectorType, STATUS_FAILED, classifyFailReason(throwable), compactionType);
    }

    public static void increaseCompactionDurationMs(String connectorType, long durationMs, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.compactionDurationMs.computeIfAbsent(normalizedType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_compaction_duration_ms_total",
                    Metric.MetricUnit.MILLISECONDS,
                    "total duration of " + connectorType + " compaction tasks");
            metric.addLabel(new MetricLabel("compaction_type", normalizedType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(durationMs);
    }

    public static void increaseCompactionInputFiles(String connectorType, long count, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.compactionInputFiles.computeIfAbsent(normalizedType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_compaction_input_files_total",
                    Metric.MetricUnit.REQUESTS,
                    "total input data files for " + connectorType + " compaction");
            metric.addLabel(new MetricLabel("compaction_type", normalizedType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(count);
    }

    public static void increaseCompactionOutputFiles(String connectorType, long count, String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.compactionOutputFiles.computeIfAbsent(normalizedType, k -> {
            LongCounterMetric metric = new LongCounterMetric(connectorType + "_compaction_output_files_total",
                    Metric.MetricUnit.REQUESTS,
                    "total output data files generated by " + connectorType + " compaction");
            metric.addLabel(new MetricLabel("compaction_type", normalizedType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(count);
    }

    public static void increaseCompactionRemovedDeleteFiles(String connectorType, long count,
                                                            String compactionType) {
        String normalizedType = normalizeCompactionType(compactionType);
        ConnectorMetrics m = getOrCreate(connectorType);
        m.compactionRemovedDeleteFiles.computeIfAbsent(normalizedType, k -> {
            LongCounterMetric metric = new LongCounterMetric(
                    connectorType + "_compaction_removed_delete_files_total",
                    Metric.MetricUnit.REQUESTS,
                    "total delete files removed during " + connectorType + " compaction");
            metric.addLabel(new MetricLabel("compaction_type", normalizedType));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(count);
    }

    // ======================= Normalization & Classification =======================

    public static String normalizeStatus(String status) {
        if (STATUS_SUCCESS.equalsIgnoreCase(status)) {
            return STATUS_SUCCESS;
        }
        if (STATUS_FAILED.equalsIgnoreCase(status)) {
            return STATUS_FAILED;
        }
        return status == null ? REASON_UNKNOWN : status;
    }

    public static String normalizeReason(String reason) {
        return reason == null ? REASON_UNKNOWN : reason;
    }

    public static String normalizeWriteType(String writeType) {
        if (WRITE_TYPE_INSERT.equalsIgnoreCase(writeType)) {
            return WRITE_TYPE_INSERT;
        }
        if (WRITE_TYPE_OVERWRITE.equalsIgnoreCase(writeType)) {
            return WRITE_TYPE_OVERWRITE;
        }
        if (WRITE_TYPE_CTAS.equalsIgnoreCase(writeType)) {
            return WRITE_TYPE_CTAS;
        }
        return writeType == null ? REASON_UNKNOWN : writeType;
    }

    public static String normalizeDeleteType(String deleteType) {
        if (DELETE_TYPE_POSITION.equalsIgnoreCase(deleteType)) {
            return DELETE_TYPE_POSITION;
        }
        if (DELETE_TYPE_METADATA.equalsIgnoreCase(deleteType)) {
            return DELETE_TYPE_METADATA;
        }
        return deleteType == null ? REASON_UNKNOWN : deleteType;
    }

    public static String normalizeCompactionType(String compactionType) {
        if (COMPACTION_TYPE_MANUAL.equalsIgnoreCase(compactionType)) {
            return COMPACTION_TYPE_MANUAL;
        }
        if (COMPACTION_TYPE_AUTO.equalsIgnoreCase(compactionType)) {
            return COMPACTION_TYPE_AUTO;
        }
        return Strings.isNullOrEmpty(compactionType) ? REASON_UNKNOWN : compactionType.toLowerCase();
    }

    public static String classifyFailReason(Throwable throwable) {
        if (throwable == null) {
            return REASON_UNKNOWN;
        }
        if (throwable instanceof OutOfMemoryError) {
            return REASON_OOM;
        }
        if (throwable instanceof AccessDeniedException) {
            return REASON_ACCESS_DENIED;
        }
        if (throwable instanceof TimeoutException) {
            return REASON_TIMEOUT;
        }
        return classifyFailReason(throwable.getMessage());
    }

    public static String classifyFailReason(String errorMessage) {
        if (Strings.isNullOrEmpty(errorMessage)) {
            return REASON_UNKNOWN;
        }
        String normalized = errorMessage.toLowerCase();
        if (normalized.contains("timeout") || normalized.contains("timed out")) {
            return REASON_TIMEOUT;
        }
        if (normalized.contains("outofmemory") || normalized.contains("out of memory")) {
            return REASON_OOM;
        }
        if (normalized.contains("access denied") || normalized.contains("permission denied")
                || normalized.contains("not authorized") || normalized.contains("unauthorized")) {
            return REASON_ACCESS_DENIED;
        }
        return REASON_UNKNOWN;
    }
}
